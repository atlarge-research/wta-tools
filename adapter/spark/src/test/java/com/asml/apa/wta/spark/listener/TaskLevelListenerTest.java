package com.asml.apa.wta.spark.listener;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.storage.RDDInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;
import scala.collection.mutable.ListBuffer;

class TaskLevelListenerTest {

  SparkContext mockedSparkContext;
  TaskLevelListener sut;

  TaskInfo testTaskInfo;

  StageInfo testStageInfo;

  SparkListenerTaskEnd taskEndEvent;

  @BeforeEach
  void setup() {
    mockedSparkContext = mock(SparkContext.class);
    when(mockedSparkContext.sparkUser()).thenReturn("testUser");
    sut = new TaskLevelListener(mockedSparkContext);

    testTaskInfo = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);

    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);

    testStageInfo = new StageInfo(
        1,
        1,
        "testStage",
        1,
        new ListBuffer<RDDInfo>().toList(),
        new ListBuffer<>().toList(),
        "details",
        new TaskMetrics(),
        new ListBuffer<Seq<TaskLocation>>().toList(),
        null,
        3);
    taskEndEvent = new SparkListenerTaskEnd(
        1, 1, "testtaskType", null, testTaskInfo, new ExecutorMetrics(), mockedMetrics);
  }

  @Test
  void testTaskEndMetricExtraction() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.addOne(testStageInfo);

    sut.onJobStart(new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties()));
    sut.onTaskEnd(taskEndEvent);
    assertEquals(1, sut.getProcessedTasks().size());
    assertEquals(1, sut.getProcessedTasks().get(0).getId());
    assertEquals("testtaskType", sut.getProcessedTasks().get(0).getType());
    assertEquals(50L, sut.getProcessedTasks().get(0).getSubmitTime());
    assertEquals(100L, sut.getProcessedTasks().get(0).getRuntime());
    assertEquals("testUser".hashCode(), sut.getProcessedTasks().get(0).getUserId());
    assertEquals(-1, sut.getProcessedTasks().get(0).getSubmissionSite());
    assertEquals("N/A", sut.getProcessedTasks().get(0).getResourceType());
    assertEquals(-1.0, sut.getProcessedTasks().get(0).getResourceAmountRequested());
    assertEquals(-1.0, sut.getProcessedTasks().get(0).getMemoryRequested());
    assertEquals(-1.0, sut.getProcessedTasks().get(0).getDiskSpaceRequested());
    assertEquals(-1L, sut.getProcessedTasks().get(0).getEnergyConsumption());
    assertEquals(-1L, sut.getProcessedTasks().get(0).getNetworkIoTime());
    assertEquals(-1L, sut.getProcessedTasks().get(0).getDiskIoTime());
    assertEquals(-1, sut.getProcessedTasks().get(0).getGroupId());
    assertEquals("", sut.getProcessedTasks().get(0).getNfrs());
    assertEquals("", sut.getProcessedTasks().get(0).getParams());
    assertEquals(0, sut.getProcessedTasks().get(0).getParents().length);
    assertEquals(0, sut.getProcessedTasks().get(0).getChildren().length);
  }
}
