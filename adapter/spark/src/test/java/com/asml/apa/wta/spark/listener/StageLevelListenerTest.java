package com.asml.apa.wta.spark.listener;

import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.storage.RDDInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.immutable.Seq;
import scala.collection.mutable.ListBuffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StageLevelListenerTest extends BaseLevelListenerTest{

    SparkListenerStageCompleted stageCompleted;

    StageInfo testStageInfo;

    @BeforeEach
    void setup() {
        ListBuffer<Integer> parents = new ListBuffer<>();
        parents.addOne(1);
        parents.addOne(2);

        TaskMetrics mockedMetrics = mock(TaskMetrics.class);
        when(mockedMetrics.executorRunTime()).thenReturn(100L);

        testStageInfo = new StageInfo(3, 0, "test", 50, null, parents.map((Function1<Integer, Object>) x -> (Object) x).toSeq(), "None", mockedMetrics, null, null, 100);

        stageCompleted = new SparkListenerStageCompleted(testStageInfo);
    }

    @Test
    void onStageCompletedTest() {
        fakeStageListener.onStageCompleted(stageCompleted);
        Map<Integer, Integer[]> ideal = new ConcurrentHashMap<>();
        ideal.put(3,new Integer[]{1, 2});
        assertTrue(equals(fakeStageListener.getStageToParents(),ideal));
    }

    @Test
    void getStageToParentsTest() {
    }

    public boolean equals(Map<Integer, Integer[]> first, Map<Integer, Integer[]> second){
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream()
                .allMatch(e -> e.getValue().equals(second.get(e.getKey())));
    }
}