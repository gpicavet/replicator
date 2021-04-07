package com.github.gpicavet.replicator;

import com.jsoniter.JsonIterator;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.function.Supplier;

@Testcontainers
class ReplicatorE2ETest {
    static Logger LOGGER = LoggerFactory.getLogger(ReplicatorE2ETest.class);

    @Container
    static PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:13-alpine")
            .withExposedPorts(5432)
//            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withUsername("postgres")
            .withPassword("postgres")
            .withClasspathResourceMapping("db/1-init.sql", "/docker-entrypoint-initdb.d/1-init.sql", BindMode.READ_ONLY)
            .withCommand("postgres -c max_wal_senders=4 " +
                    "-c wal_keep_size=4 " +
                    "-c wal_level=logical " +
                    "-c max_replication_slots=4");
    @Container
    static ElasticsearchContainer elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.12.0")
            .withExposedPorts(9200)
//            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withEnv("discovery.type", "single-node");


    @Test
    void shouldReplicateUpdate() throws Exception {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresql.getJdbcUrl());
        dataSource.setUser(postgresql.getUsername());
        dataSource.setPassword(postgresql.getPassword());
        dataSource.setProperty(PGProperty.ASSUME_MIN_SERVER_VERSION, "9.4");
        dataSource.setProperty(PGProperty.REPLICATION, "database");
        dataSource.setProperty(PGProperty.PREFER_QUERY_MODE, "simple");

        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", elastic.getMappedPort(9200), "http")).build();

        //init data
        try (Connection sqlConnection = dataSource.getConnection();) {
            Statement st = sqlConnection.createStatement();
            for (int i = 0; i < 10; i++) {
                st.execute("insert into question values(" + i + ",'title','body','author',NOW())");
                for (int j = 0; j < 10; j++) {
                    st.execute("insert into answer values(" + (10 * i + j) + "," + i + ",'body','author',NOW())");
                }
            }
        }

        //start replication stream
        Replicator replicator = new Replicator("sync.yml", dataSource, new SqlProcessor(dataSource, "sync.yml"), new EsWriter(restClient));
        replicator.start();

        //make some updates
        try (Connection sqlConnection = dataSource.getConnection();) {
            Statement st = sqlConnection.createStatement();
            st.execute("update question set que_creationDate=NOW()");
        }

        //assert elastic search is sync
        assertWait(() -> {
            try {
                Response resp = restClient.performRequest(new Request("GET", "/questions/_count"));
                int count = JsonIterator.deserialize(IOUtils.toByteArray(resp.getEntity().getContent())).get("count").toInt();
                return count == 100;
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    throw new RuntimeException(e);
                }
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 10000);

        replicator.stop();
    }

    void assertWait(Supplier<Boolean> f, long timeout) throws InterruptedException {
        for (int i = 0; i < timeout / 1000; i++) {
            Thread.sleep(1000);
            if (f.get()) {
                return;
            }
        }
        Assertions.fail("assertEquals timeout");
    }
}
