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
import org.apache.spark.executor.ShuffleWriteMetrics;
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

  TaskInfo testTaskInfo1;

  TaskInfo testTaskInfo2;

  StageInfo testStageInfo;

  SparkListenerTaskEnd taskEndEvent;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerStageCompleted stageCompleted;

  @BeforeEach
  void setup() {
    testTaskInfo1 = new TaskInfo(0, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);

    ListBuffer<Integer> parents = new ListBuffer<>();
    parents.$plus$eq(1);
    parents.$plus$eq(2);
    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics = mock(ShuffleWriteMetrics.class);
    when(mockedMetrics.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics);
    when(mockedShuffleMetrics.bytesWritten()).thenReturn(100L);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);
    when(mockedMetrics.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics.diskBytesSpilled()).thenReturn(-1L);
    when(mockedMetrics.resultSerializationTime()).thenReturn(-1L);
    when(mockedMetrics.executorDeserializeTime()).thenReturn(0L);

    testStageInfo = new StageInfo(
        2,
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
        2, 1, "testTaskType", null, testTaskInfo1, new ExecutorMetrics(), mockedMetrics);
    taskEndEvent2 = new SparkListenerTaskEnd(
        2, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics);
    stageCompleted = new SparkListenerStageCompleted(testStageInfo);
  }

  @Test
  void testTaskStageMappings() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo);

    fakeTaskListener1.onJobStart(new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties()));
    fakeTaskListener1.onTaskEnd(taskEndEvent);
    assertThat(fakeTaskListener1.getStageToTasks().size()).isEqualTo(1);
    List<Task> list = new ArrayList<>();
    list.add(Task.builder().id(1L).build());
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).size()).isEqualTo(1);
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).get(0).getId()).isEqualTo(1);
    assertThat(fakeTaskListener1.getTaskToStage().size()).isEqualTo(1);
    assertThat(fakeTaskListener1.getTaskToStage()).containsEntry(1L, 3L);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    assertThat(fakeTaskListener1.getStageToTasks().size()).isEqualTo(1);
    list.add(Task.builder().id(2L).build());
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).size()).isEqualTo(2);
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).get(0).getId()).isEqualTo(1);
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).get(1).getId()).isEqualTo(2);
    assertThat(fakeTaskListener1.getTaskToStage().size()).isEqualTo(2);
    assertThat(fakeTaskListener1.getTaskToStage()).containsEntry(1L, 3L);
    assertThat(fakeTaskListener1.getTaskToStage()).containsEntry(2L, 3L);
  }

  @Test
  void testTaskEndMetricExtraction() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo);

    fakeTaskListener1.onJobStart(new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties()));
    fakeTaskListener1.onTaskEnd(taskEndEvent);
    assertEquals(1, fakeTaskListener1.getProcessedObjects().count());
    Task curTask = fakeTaskListener1.getProcessedObjects().head();
    assertEquals(1, curTask.getId());
    assertEquals("testTaskType", curTask.getType());
    assertEquals(50L, curTask.getTsSubmit());
    assertEquals(100L, curTask.getRuntime());
    assertEquals(1L, curTask.getWorkflowId());
    assertEquals("testUser".hashCode(), curTask.getUserId());
    assertEquals(-1, curTask.getSubmissionSite());
    assertEquals("N/A", curTask.getResourceType());
    assertEquals(-1.0, curTask.getResourceAmountRequested());
    assertEquals(-1.0, curTask.getMemoryRequested());
    assertEquals(99.0, curTask.getDiskSpaceRequested());
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
