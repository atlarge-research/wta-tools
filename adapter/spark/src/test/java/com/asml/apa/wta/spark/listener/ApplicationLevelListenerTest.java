package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workload;
import java.util.Properties;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskLocality;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

class ApplicationLevelListenerTest extends BaseLevelListenerTest {

  SparkListenerApplicationEnd applicationEndObj;

  TaskInfo testTaskInfo;

  TaskInfo testTaskInfo2;

  TaskInfo testTaskInfo3;

  TaskInfo testTaskInfo4;

  StageInfo testStageInfo;

  StageInfo testStageInfo2;

  SparkListenerTaskEnd taskEndEvent;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerTaskEnd taskEndEvent3;

  SparkListenerTaskEnd taskEndEvent4;

  SparkListenerTaskStart taskStartEvent;

  SparkListenerTaskStart taskStartEvent2;

  SparkListenerTaskStart taskStartEvent3;

  SparkListenerTaskStart taskStartEvent4;

  SparkListenerStageCompleted stageCompleted;

  SparkListenerStageCompleted stageCompleted2;

  @BeforeEach
  void setup() {

    testTaskInfo = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(2, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo3 = new TaskInfo(3, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo4 = new TaskInfo(4, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    ListBuffer<Object> parents = new ListBuffer<>();
    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);

    testStageInfo =
        new StageInfo(5, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics, null, null, 100);
    parents.$plus$eq(5);
    testStageInfo2 = new StageInfo(6, 0, "test", 50, null, parents, "None", mockedMetrics, null, null, 100);
    taskEndEvent = new SparkListenerTaskEnd(
        5, 1, "testTaskType", null, testTaskInfo, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent2 = new SparkListenerTaskEnd(
        5, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent3 = new SparkListenerTaskEnd(
        6, 1, "testTaskType", null, testTaskInfo3, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent4 = new SparkListenerTaskEnd(
        6, 1, "testTaskType", null, testTaskInfo4, new ExecutorMetrics(), mockedMetrics);
    taskStartEvent = new SparkListenerTaskStart(5, 1, testTaskInfo);
    taskStartEvent2 = new SparkListenerTaskStart(5, 1, testTaskInfo2);
    taskStartEvent3 = new SparkListenerTaskStart(6, 1, testTaskInfo3);
    taskStartEvent4 = new SparkListenerTaskStart(6, 1, testTaskInfo4);

    stageCompleted = new SparkListenerStageCompleted(testStageInfo);
    stageCompleted2 = new SparkListenerStageCompleted(testStageInfo2);
    applicationEndObj = new SparkListenerApplicationEnd(mockedSparkContext.startTime() + 1000L);
  }

  @Test
  void parentChildrenAggregationTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeTaskListener.onTaskStart(taskStartEvent);
    fakeTaskListener.onTaskEnd(taskEndEvent);
    fakeTaskListener.onTaskStart(taskStartEvent2);
    fakeTaskListener.onTaskEnd(taskEndEvent2);
    fakeStageListener.onStageCompleted(stageCompleted);
    fakeTaskListener.onTaskStart(taskStartEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskStart(taskStartEvent4);
    fakeTaskListener.onTaskEnd(taskEndEvent4);
    fakeStageListener.onStageCompleted(stageCompleted2);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    Task task = fakeTaskListener.getProcessedObjects().get(0);
    assertThat(task.getParents().length).isEqualTo(0);
    assertThat(task.getChildren().length).isEqualTo(2);
    assertThat(task.getChildren()).contains(4, 5);
    task = fakeTaskListener.getProcessedObjects().get(1);
    assertThat(task.getParents().length).isEqualTo(0);
    assertThat(task.getChildren().length).isEqualTo(2);
    assertThat(task.getChildren()).contains(4, 5);
    task = fakeTaskListener.getProcessedObjects().get(2);
    assertThat(task.getParents().length).isEqualTo(2);
    assertThat(task.getParents()).contains(2, 3);
    assertThat(task.getChildren().length).isEqualTo(0);
    task = fakeTaskListener.getProcessedObjects().get(3);
    assertThat(task.getParents().length).isEqualTo(2);
    assertThat(task.getParents()).contains(2, 3);
    assertThat(task.getChildren().length).isEqualTo(0);
  }

  @Test
  void applicationListenerCollectsDesiredInformation() {
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getDomain()).isEqualTo(fakeConfig.getDomain());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getDateStart()).isEqualTo(sutStartTime);
    assertThat(workload.getDateEnd()).isEqualTo(sutStartTime + 1000L);
  }
}
