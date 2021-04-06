package com.github.gpicavet.replicator;

import com.github.javafaker.Faker;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@Testcontainers
class PerfInit {
    static Logger LOGGER = LoggerFactory.getLogger(PerfInit.class);

    public static void main(String[] args) throws Exception {

        Faker faker = new Faker();

        try(Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "postgres", "postgres");
            RestClient restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http")).build()) {

            restClient.performRequest(new Request("DELETE", "questions"));
            conn.createStatement().execute("truncate question cascade");


            PreparedStatement preparedStatement =
                    conn.prepareStatement("insert into question values(?,?,?,?,NOW())");

            int pk=1;
            for(int b=1; b<=1000; b++) {
                for (int i = 0; i < 1000; i++) {
                    preparedStatement.setInt(1, pk++);
                    preparedStatement.setString(2, faker.shakespeare().asYouLikeItQuote());
                    preparedStatement.setString(3, faker.lorem().paragraph(10));
                    preparedStatement.setString(4, faker.name().fullName());
                    preparedStatement.addBatch();

                }
                preparedStatement.executeBatch();
                LOGGER.info("{} questions inserted...", b*1000);
            }


        }

    }
}
