package org.flinkdsl.flow;


import org.apache.flink.streaming.api.functions.async.AsyncFunction;

public interface AsyncEnricher<IN, OUT> extends AsyncFunction<IN, OUT> { }
