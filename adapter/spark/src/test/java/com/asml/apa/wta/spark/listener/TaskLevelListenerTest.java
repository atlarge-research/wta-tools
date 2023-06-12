package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskLocality;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.mutable.ListBuffer;

class TaskLevelListenerTest extends BaseLevelListenerTest {

  TaskInfo testTaskInfo;

  TaskInfo testTaskInfo2;

  StageInfo testStageInfo;

  SparkListenerTaskEnd taskEndEvent;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerStageCompleted stageCompleted;

  @BeforeEach
  void setup() {

    testTaskInfo = new TaskInfo(0, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);

    ListBuffer<Integer> parents = new ListBuffer<>();
    parents.$plus$eq(0);
    parents.$plus$eq(1);
    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);

    testStageInfo = new StageInfo(
        3,
        0,
        "test",
        50,
        null,
        JavaConverters.collectionAsScalaIterable(JavaConverters.asJavaCollection(parents).stream()
                .map(x -> (Object) x)
                .collect(Collectors.toList()))
            .toList(),
        "None",
        mockedMetrics,
        null,
        null,
        100);
    parents.$plus$eq(3);
    taskEndEvent = new SparkListenerTaskEnd(
        3, 1, "testTaskType", null, testTaskInfo, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent2 = new SparkListenerTaskEnd(
        3, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics);

    stageCompleted = new SparkListenerStageCompleted(testStageInfo);
  }

  @Test
  void testTaskStageMappings() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo);

    fakeTaskListener.onJobStart(new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties()));
    fakeTaskListener.onTaskEnd(taskEndEvent);
    assertThat(fakeTaskListener.getStageToTasks().size()).isEqualTo(1);
    List<Long> list = new ArrayList<>();
    list.add(1L);
    assertThat(fakeTaskListener.getStageToTasks()).containsEntry(3, list);
    assertThat(fakeTaskListener.getTaskToStage().size()).isEqualTo(1);
    assertThat(fakeTaskListener.getTaskToStage()).containsEntry(1L, 3);
    fakeTaskListener.onTaskEnd(taskEndEvent2);
    assertThat(fakeTaskListener.getStageToTasks().size()).isEqualTo(1);
    list.add(2L);
    assertThat(fakeTaskListener.getStageToTasks()).containsEntry(3, list);
    assertThat(fakeTaskListener.getTaskToStage().size()).isEqualTo(2);
    assertThat(fakeTaskListener.getTaskToStage()).containsEntry(1L, 3);
    assertThat(fakeTaskListener.getTaskToStage()).containsEntry(2L, 3);
  }

  @Test
  void testTaskEndMetricExtraction() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo);

    fakeTaskListener.onJobStart(new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties()));
    fakeTaskListener.onTaskEnd(taskEndEvent);
    assertEquals(1, fakeTaskListener.getProcessedObjects().size());
    Task curTask = fakeTaskListener.getProcessedObjects().get(0);
    assertEquals(1, curTask.getId());
    assertEquals("testTaskType", curTask.getType());
    assertEquals(50L, curTask.getTsSubmit());
    assertEquals(100L, curTask.getRuntime());
    assertEquals(2L, curTask.getWorkflowId());
    assertEquals("testUser".hashCode(), curTask.getUserId());
    assertEquals(-1, curTask.getSubmissionSite());
    assertEquals("N/A", curTask.getResourceType());
    assertEquals(-1.0, curTask.getResourceAmountRequested());
    assertEquals(-1.0, curTask.getMemoryRequested());
    assertEquals(-1.0, curTask.getDiskSpaceRequested());
    assertEquals(-1L, curTask.getEnergyConsumption());
    assertEquals(-1L, curTask.getNetworkIoTime());
    assertEquals(-1L, curTask.getDiskIoTime());
    assertEquals(-1, curTask.getGroupId());
    assertEquals("", curTask.getNfrs());
    assertEquals("", curTask.getParams());
    assertEquals(0, curTask.getParents().length);
    assertEquals(0, curTask.getChildren().length);
  }
}
