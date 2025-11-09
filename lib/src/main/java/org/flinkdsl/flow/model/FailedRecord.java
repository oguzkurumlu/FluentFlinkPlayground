package org.flinkdsl.flow.model;

public record FailedRecord<T>(T value, String reason, Throwable error) { }