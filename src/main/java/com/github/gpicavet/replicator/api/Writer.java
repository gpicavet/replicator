package com.github.gpicavet.replicator.api;

import java.io.IOException;
import java.util.List;

public interface Writer {

    void write(List<Document> docs) throws IOException;

}
