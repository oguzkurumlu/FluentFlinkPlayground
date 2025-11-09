package org.flinkdsl.flow.steps;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

public final class Done {
    private final StreamExecutionEnvironment env;

    public Done(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public JobExecutionResult execute(String jobName) throws Exception {
        return env.execute(jobName);
    }

    public StreamGraph inspectPlan() {
        return env.getStreamGraph();
    }
}
