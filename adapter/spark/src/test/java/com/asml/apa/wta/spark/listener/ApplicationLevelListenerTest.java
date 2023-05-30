package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workload;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

import java.util.Properties;

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

  SparkListenerStageCompleted stageCompleted;

  SparkListenerStageCompleted stageCompleted2;

  @BeforeEach
  void setup() {

    testTaskInfo = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(2, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo3 = new TaskInfo(3, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo4 = new TaskInfo(4, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    ListBuffer<Integer> parents = new ListBuffer<>();
    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);

    testStageInfo = new StageInfo(
            5, 0, "test", 50, null, parents.toList().map(x -> x), "None", mockedMetrics, null, null, 100);
    parents.addOne(5);
    testStageInfo2 = new StageInfo(
            6, 0, "test", 50, null, parents.toList().map(x -> x), "None", mockedMetrics, null, null, 100);
    taskEndEvent = new SparkListenerTaskEnd(
            5, 1, "testTaskType", null, testTaskInfo, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent2 = new SparkListenerTaskEnd(
            5, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent3 = new SparkListenerTaskEnd(
            6, 1, "testTaskType", null, testTaskInfo3, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent4 = new SparkListenerTaskEnd(
            6, 1, "testTaskType", null, testTaskInfo4, new ExecutorMetrics(), mockedMetrics);


    stageCompleted = new SparkListenerStageCompleted(testStageInfo);
    stageCompleted2 = new SparkListenerStageCompleted(testStageInfo2);
    applicationEndObj = new SparkListenerApplicationEnd(mockedSparkContext.startTime() + 1000L);
  }

  @Test
  void parentChildrenAggregationTest(){
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.addOne(testStageInfo);
    stageBuffer.addOne(testStageInfo2);

    fakeTaskListener.onJobStart(new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties()));
    fakeTaskListener.onTaskEnd(taskEndEvent);
    fakeTaskListener.onTaskEnd(taskEndEvent2);
    fakeStageListener.onStageCompleted(stageCompleted);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent4);
    fakeStageListener.onStageCompleted(stageCompleted2);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    Task task = fakeTaskListener.processedObjects.get(0);
    assertThat(task.getParents().length).isEqualTo(0);
    assertThat(task.getChildren().length).isEqualTo(2);
    assertThat(task.getChildren()).contains(3, 4);
  }

  void applicationListenerCollectsDesiredInformation() {
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(workload.getWorkflows()).hasSize(0);
    assertThat(workload.getWorkflows().length).isEqualTo(workload.getTotalWorkflows());
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getDomain()).isEqualTo(fakeConfig.getDomain());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getDateStart()).isEqualTo(sutStartTime);
    assertThat(workload.getDateEnd()).isEqualTo(sutStartTime + 1000L);
  }
}
