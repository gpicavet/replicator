package com.github.gpicavet.replicator.api;

import java.util.List;

public interface Processor {

    List<Document> process(List<Event> events);

}
