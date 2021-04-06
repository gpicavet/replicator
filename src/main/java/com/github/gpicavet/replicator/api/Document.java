package com.github.gpicavet.replicator.api;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class Document {
    private String idField;
    private String type;

    private Map<String, Object> fields = new HashMap<>();
}
