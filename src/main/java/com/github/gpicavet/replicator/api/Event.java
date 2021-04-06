package com.github.gpicavet.replicator.api;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Event {
    private final String table;
    private final String command;
    private final Object pk;
}
