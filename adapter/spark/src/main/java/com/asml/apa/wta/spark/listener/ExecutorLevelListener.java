package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class ExecutorLevelListener extends AbstractListener<Task>{

    private final Map<String, Integer> executorResources = new ConcurrentHashMap<>();

    public ExecutorLevelListener(SparkContext sparkContext, RuntimeConfig config) {
        super(sparkContext, config);
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
        executorResources.put(executorAdded.executorId(),executorAdded.executorInfo().resourceProfileId());
    }
}
