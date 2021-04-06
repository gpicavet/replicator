package com.github.gpicavet.replicator;

import com.github.gpicavet.replicator.api.Document;
import com.github.gpicavet.replicator.api.Writer;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsoniterSpi;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch default writer
 */
public class EsWriter implements Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsWriter.class);

    RestClient restClient;

    public EsWriter(RestClient restClient) {
        this.restClient = restClient;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        JsoniterSpi.registerTypeEncoder(Timestamp.class, (obj, stream) -> {
            stream.writeVal(sdf.format(obj));
        });
    }

    @Override
    public void write(List<Document> docs) throws IOException {
        LOGGER.info("writing {} documents to Elastic", docs.size());

        //using bulk Api to speed up indexing
        StringBuilder sb = new StringBuilder();
        for (Document doc : docs) {
            sb.append(JsonStream.serialize(Map.of("index",
                    Map.of("_index", doc.getType(),
                            "_id", doc.getFields().get(doc.getIdField())))));
            sb.append("\r\n");
            sb.append(JsonStream.serialize(doc.getFields()));
            sb.append("\r\n");
        }

        Request request = new Request("POST", "/_bulk");
        request.setEntity(new StringEntity(sb.toString(), ContentType.create("application/x-ndjson", "UTF-8")));

        this.restClient.performRequest(request);

    }
}
