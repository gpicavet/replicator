package com.github.gpicavet.replicator;

import com.github.gpicavet.replicator.api.Event;
import com.github.gpicavet.replicator.api.Processor;
import com.github.gpicavet.replicator.api.Writer;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public class Replicator implements Runnable {
    static Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    private static final String DEFAULT_REPLICATION_SLOT = "DEFAULT_REPLICATION_SLOT";

    private final Map<String, Object> config;

    private final Map<String, EventConsumer> workerByTable = new HashMap<>();
    private final List<Thread> threadPool = new ArrayList<>();
    private final DataSource replicationDatasource;

    private Connection conn;
    private PGReplicationStream stream;
    private boolean stop = false;

    public Replicator(String configFile, DataSource replicationDatasource, Processor processor, Writer writer) throws Exception {

        this.replicationDatasource = replicationDatasource;

        threadPool.add(new Thread(this));

        Yaml yaml = new Yaml();
        this.config = yaml.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
        for (String table : this.config.keySet()) {
            EventConsumer w = new EventConsumer();
            w.setTable(table);
            w.setProcessor(processor);
            w.setWriter(writer);

            workerByTable.put(table, w);
            threadPool.add(new Thread(w));
        }
    }

    @SneakyThrows
    @Override
    public void run() {
        Pattern p = Pattern.compile("table (\\S+)\\.(\\S+): (\\S+): \\S+:(\\S+) .*");

        while (!stop) {
            //non blocking receive message
            ByteBuffer msg = stream.readPending();

            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            String s = new String(source, offset, length);

            if (!s.equals("BEGIN") && !s.equals("COMMIT")) {

                Matcher m = p.matcher(s);
                if (m.matches()) {
                    String schema = m.group(1);
                    String table = m.group(2);
                    String command = m.group(3);
                    String pk = m.group(4);

                    if (workerByTable.containsKey(table)) {
                        workerByTable.get(table).getQueue().add(new Event(table, command, pk));
                    }
                }
            }

            //feedback to flush WAL
            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());

        }

    }

    public void start() throws SQLException {
        this.conn = replicationDatasource.getConnection();

        PGConnection replConnection = conn.unwrap(PGConnection.class);

        try {
            replConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(DEFAULT_REPLICATION_SLOT)
                    .withOutputPlugin("test_decoding")
                    .make();
        } catch (SQLException ex) {
            LOGGER.info("replication slot already exists");
        }

        this.stream =
                replConnection.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(DEFAULT_REPLICATION_SLOT)
                        .withSlotOption("include-xids", false)
                        .withSlotOption("skip-empty-xacts", true)
                        .withStatusInterval(20, TimeUnit.SECONDS)
                        .start();

        for (Thread t : threadPool)
            t.start();
        LOGGER.info("Replicator started");
    }

    public void stop() {
        try {
            stop = true;
            for (EventConsumer w : workerByTable.values())
                w.setStop(true);
            for (Thread t : threadPool)
                t.join();

            conn.close();
        } catch (Exception throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public static void main(String[] args) throws Exception {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setProperty(PGProperty.PG_HOST, getenv("POSTGRES_HOST", "localhost"));
        dataSource.setProperty(PGProperty.PG_PORT, getenv("POSTGRES_PORT", "5432"));
        dataSource.setProperty(PGProperty.ASSUME_MIN_SERVER_VERSION, "9.4");
        dataSource.setProperty(PGProperty.REPLICATION, "database");
        dataSource.setProperty(PGProperty.PREFER_QUERY_MODE, "simple");
        dataSource.setUser(getenv("POSTGRES_USER", "postgres"));
        dataSource.setPassword(getenv("POSTGRES_PASS", "postgres"));

        RestClient restClient = RestClient.builder(
                new HttpHost(getenv("ELASTIC_HOST", "localhost"),
                        Integer.parseInt(getenv("ELASTIC_PORT", "9200")),
                        getenv("ELASTIC_SCHEME", "http"))).build();

        Replicator repl = new Replicator(
                "sync.yml",
                dataSource,
                new SqlProcessor(dataSource, "sync.yml"),
                new EsWriter(restClient));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            repl.stop();
        }));

        repl.start();

    }

    static String getenv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    @Getter
    @Setter
    class EventConsumer implements Runnable {
        private BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
        private String table;
        private Processor processor;
        private Writer writer;
        private boolean stop = false;
        private int chunkSize = 100;

        @SneakyThrows
        @Override
        public void run() {
            while (true) {
                List<Event> eventList = new ArrayList<>();
                Event e = queue.poll(1000L, TimeUnit.MILLISECONDS);
                if (e != null) {
                    eventList.add(e);
                    while (eventList.size() < chunkSize) {
                        e = queue.poll();
                        if (e == null)
                            break;
                        eventList.add(e);
                    }

                    writer.write(processor.process(eventList));
                } else {
                    //stop if there is no more event in queue
                    if (stop)
                        break;
                }
            }
        }
    }

}