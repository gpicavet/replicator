package com.github.gpicavet.replicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;

@Testcontainers
class PerfUpdate {
    static Logger LOGGER = LoggerFactory.getLogger(PerfUpdate.class);

    public static void main(String[] args) throws Exception {

        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "postgres", "postgres");) {
            conn.createStatement().execute("update answer set ans_creationDate=NOW() where ans_id <= 10000");
        }

    }
}
