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
class PerfUpdate {
    static Logger LOGGER = LoggerFactory.getLogger(PerfUpdate.class);

    public static void main(String[] args) throws Exception {

        try(Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "postgres", "postgres");) {
            conn.createStatement().execute("update question set que_creationDate=NOW()");
        }

    }
}
