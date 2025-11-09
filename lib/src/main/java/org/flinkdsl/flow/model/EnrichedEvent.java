package org.flinkdsl.flow.model;

import java.util.Map;

public record EnrichedEvent<T>(T original, Map<String, Object> attrs) { }