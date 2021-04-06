package com.github.gpicavet.replicator;

import com.github.gpicavet.replicator.api.Document;
import com.github.gpicavet.replicator.api.Event;
import com.github.gpicavet.replicator.api.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transform a list of replication events to a list of documents
 * use sync.yml sql queries to fetch document data
 */
public class SqlProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlProcessor.class);

    private final Map<String, Object> config;
    private final DataSource dataSource;

    public SqlProcessor(DataSource dataSource, String configFile) {
        this.dataSource = dataSource;
        Yaml yaml = new Yaml();
        this.config = yaml.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
    }

    @Override
    public List<Document> process(List<Event> events) {
        String table = events.get(0).getTable();
        LOGGER.info("processing {} elements of table {}", events.size(), table);

        Map<String, Object> configTable = (Map<String, Object>) config.get(table);
        String idField = (String) configTable.get("document-id");
        String documentType = (String) configTable.get("document-type");
        String sql = (String) configTable.get("sql");

        String sqlIn = events.stream().map(pk -> "?").collect(Collectors.joining(","));
        sql = sql.replace(":IDS", sqlIn);

        List<Document> docs = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stat = conn.prepareStatement(sql);) {

            for (int i = 0; i < events.size(); i++) {
                stat.setObject(i+1, events.get(i).getPk());
            }

            ResultSet rs = stat.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next()) {
                Document doc = new Document();
                doc.setType(documentType);
                doc.setIdField(idField);

                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    doc.getFields().put(rsmd.getColumnName(i+1), rs.getObject(i+1));
                }

                docs.add(doc);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return docs;
    }
}
