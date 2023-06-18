package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workload;
import java.util.Properties;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskLocality;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

class ApplicationLevelListenerTest extends BaseLevelListenerTest {

  SparkListenerApplicationEnd applicationEndObj;

  TaskInfo testTaskInfo1;

  TaskInfo testTaskInfo2;

  TaskInfo testTaskInfo3;

  TaskInfo testTaskInfo4;

  StageInfo testStageInfo1;

  StageInfo testStageInfo2;

  StageInfo testStageInfo3;

  SparkListenerTaskEnd taskEndEvent1;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerTaskEnd taskEndEvent3;

  SparkListenerTaskEnd taskEndEvent4;

  SparkListenerStageCompleted stageCompleted1;

  SparkListenerStageCompleted stageCompleted2;

  SparkListenerStageCompleted stageCompleted3;

  int stageId1;
  int stageId2;
  int stageId3;

  long applicationDateEnd;

  @BeforeEach
  void setup() {
    testTaskInfo1 = new TaskInfo(0, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo3 = new TaskInfo(2, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo4 = new TaskInfo(3, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    ListBuffer<Object> parents = new ListBuffer<>();

    ShuffleWriteMetrics mockedShuffleMetrics1 = mock(ShuffleWriteMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics2 = mock(ShuffleWriteMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics3 = mock(ShuffleWriteMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics4 = mock(ShuffleWriteMetrics.class);
    when(mockedShuffleMetrics1.bytesWritten()).thenReturn(100L);
    when(mockedShuffleMetrics2.bytesWritten()).thenReturn(-1L);
    when(mockedShuffleMetrics3.bytesWritten()).thenReturn(0L);
    when(mockedShuffleMetrics4.bytesWritten()).thenReturn(50L);

    TaskMetrics mockedMetrics1 = mock(TaskMetrics.class);
    when(mockedMetrics1.peakExecutionMemory()).thenReturn(100L);
    when(mockedMetrics1.executorRunTime()).thenReturn(100L);
    when(mockedMetrics1.diskBytesSpilled()).thenReturn(100L);
    when(mockedMetrics1.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics1);

    TaskMetrics mockedMetrics2 = mock(TaskMetrics.class);
    when(mockedMetrics2.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics2.diskBytesSpilled()).thenReturn(-1L);
    when(mockedMetrics2.executorRunTime()).thenReturn(-1L);
    when(mockedMetrics2.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics2);

    TaskMetrics mockedMetrics3 = mock(TaskMetrics.class);
    when(mockedMetrics3.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics3.diskBytesSpilled()).thenReturn(0L);
    when(mockedMetrics3.executorRunTime()).thenReturn(-1L);
    when(mockedMetrics3.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics3);

    TaskMetrics mockedMetrics4 = mock(TaskMetrics.class);
    when(mockedMetrics4.peakExecutionMemory()).thenReturn(30L);
    when(mockedMetrics4.diskBytesSpilled()).thenReturn(20L);
    when(mockedMetrics4.executorRunTime()).thenReturn(80L);
    when(mockedMetrics4.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics4);

    stageId1 = 2;
    stageId2 = 10;
    stageId3 = 14;
    parents.$plus$eq(stageId1);

    testStageInfo1 = new StageInfo(
        stageId1, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics1, null, null, 100);
    testStageInfo2 = new StageInfo(stageId2, 0, "test", 50, null, parents, "None", mockedMetrics1, null, null, 100);
    testStageInfo3 = new StageInfo(stageId3, 0, "test", 50, null, parents, "None", mockedMetrics1, null, null, 100);

    taskEndEvent1 = new SparkListenerTaskEnd(
        stageId1, 1, "testTaskType", null, testTaskInfo1, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent2 = new SparkListenerTaskEnd(
        stageId1, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent3 = new SparkListenerTaskEnd(
        stageId2, 1, "testTaskType", null, testTaskInfo3, new ExecutorMetrics(), mockedMetrics2);
    taskEndEvent4 = new SparkListenerTaskEnd(
        stageId2, 1, "testTaskType", null, testTaskInfo4, new ExecutorMetrics(), mockedMetrics3);
    stageCompleted1 = new SparkListenerStageCompleted(testStageInfo1);
    stageCompleted2 = new SparkListenerStageCompleted(testStageInfo2);
    stageCompleted3 = new SparkListenerStageCompleted(testStageInfo3);

    applicationDateEnd = mockedSparkContext.startTime() + 1000L;
    applicationEndObj = new SparkListenerApplicationEnd(applicationDateEnd);
  }

  @Test
  void workloadBuiltWithDefaultMetricValues() {
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener1.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener1.getProcessedObjects().size()).isEqualTo(1);
    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMaxResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getCovResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getStdMemory()).isEqualTo(-1);
    assertThat(workload.getMedianMemory()).isEqualTo(-1);
    assertThat(workload.getMinMemory()).isEqualTo(-1);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1);
  }

  @Test
  void workloadGeneralMetricsCollected() {
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener1.getProcessedObjects().get(0);
    assertThat(workload.getDomain()).isEqualTo(fakeConfig1.getDomain());
    assertThat(workload.getAuthors()).isEqualTo(fakeConfig1.getAuthors());
    assertThat(workload.getWorkloadDescription()).isEqualTo(fakeConfig1.getDescription());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getDateStart()).isEqualTo(sutStartTime);
    assertThat(workload.getDateEnd()).isEqualTo(applicationDateEnd);
  }

  @Test
  void workloadCountMetricsCollected() {
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener1.getProcessedObjects().get(0);
    assertThat(workload.getTotalWorkflows()).isEqualTo(0);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getNumUsers()).isEqualTo(0);
    assertThat(workload.getNumGroups()).isEqualTo(0);
    assertThat(workload.getNumSites()).isEqualTo(0);
    assertThat(workload.getNumResources()).isEqualTo(-1L);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1L);
  }

  @Test
  void workloadResourceMetricsCollected() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener1.onJobStart(jobStart);
    fakeStageListener1.onJobStart(jobStart);
    fakeTaskListener1.onTaskEnd(taskEndEvent1);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    fakeStageListener1.onStageCompleted(stageCompleted1);
    fakeTaskListener1.onTaskEnd(taskEndEvent3);
    fakeTaskListener1.onTaskEnd(taskEndEvent4);
    fakeStageListener1.onStageCompleted(stageCompleted2);

    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener1.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener1.getProcessedObjects().size()).isEqualTo(1);
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(20);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(133.33333333333334);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(4000);
  }

  @Test
  void stageWithoutTaskTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener1.onJobStart(jobStart);
    fakeStageListener1.onJobStart(jobStart);
    fakeStageListener1.onStageCompleted(stageCompleted2);

    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener1.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener1.getProcessedObjects().size()).isEqualTo(1);
    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(-1);
    assertThat(workload.getMaxResourceTask()).isEqualTo(-1);
    assertThat(workload.getCovResourceTask()).isEqualTo(-1);
    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getStdMemory()).isEqualTo(-1);
    assertThat(workload.getMedianMemory()).isEqualTo(-1);
    assertThat(workload.getMinMemory()).isEqualTo(-1);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(-1);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1);
  }

  @Test
  void applicationEndEnteringForbiddenBranchWillGracefullyTerminate() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart1 = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    SparkListenerJobStart jobStart2 = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());

    fakeTaskListener1.onJobStart(jobStart1);
    fakeStageListener1.onJobStart(jobStart1);
    fakeTaskListener1.onTaskEnd(taskEndEvent1);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    fakeStageListener1.onStageCompleted(stageCompleted1);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);

    fakeTaskListener1.onJobStart(jobStart2);
    fakeStageListener1.onJobStart(jobStart2);
    fakeTaskListener1.onTaskEnd(taskEndEvent3);
    fakeTaskListener1.onTaskEnd(taskEndEvent4);
    fakeStageListener1.onStageCompleted(stageCompleted2);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects()).hasSize(1);
  }

  @Test
  void parentChildrenAggregatedForTaskLevelMetrics() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener1.onJobStart(jobStart);
    fakeStageListener1.onJobStart(jobStart);
    fakeTaskListener1.onTaskEnd(taskEndEvent1);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    fakeStageListener1.onStageCompleted(stageCompleted1);
    fakeTaskListener1.onTaskEnd(taskEndEvent3);
    fakeTaskListener1.onTaskEnd(taskEndEvent4);
    fakeStageListener1.onStageCompleted(stageCompleted2);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeTaskListener1.getProcessedObjects()).hasSize(4);

    Task task1 = fakeTaskListener1.getProcessedObjects().get(0);
    assertThat(task1.getParents().length).isEqualTo(0);
    assertThat(task1.getChildren().length).isEqualTo(2);
    assertThat(task1.getChildren()).contains(3L, 4L);

    Task task2 = fakeTaskListener1.getProcessedObjects().get(1);
    assertThat(task2.getParents().length).isEqualTo(0);
    assertThat(task2.getChildren().length).isEqualTo(2);
    assertThat(task2.getChildren()).contains(3L, 4L);

    Task task3 = fakeTaskListener1.getProcessedObjects().get(2);
    assertThat(task3.getParents().length).isEqualTo(2);
    assertThat(task3.getParents()).contains(1L, 2L);
    assertThat(task3.getChildren().length).isEqualTo(0);

    Task task4 = fakeTaskListener1.getProcessedObjects().get(3);
    assertThat(task4.getParents().length).isEqualTo(2);
    assertThat(task4.getParents()).contains(1L, 2L);
    assertThat(task4.getChildren().length).isEqualTo(0);
  }

  @Test
  void parentChildrenAggregatedForStageLevelMetrics() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    stageBuffer.$plus$eq(testStageInfo3);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    fakeStageListener2.onJobStart(jobStart);
    fakeStageListener2.onStageCompleted(stageCompleted1);
    fakeStageListener2.onStageCompleted(stageCompleted2);
    fakeStageListener2.onStageCompleted(stageCompleted3);
    fakeApplicationListener2.onApplicationEnd(applicationEndObj);
    assertThat(fakeStageListener2.getProcessedObjects()).hasSize(3);

    Task task1 = fakeStageListener2.getProcessedObjects().get(0);
    assertThat(task1.getParents().length).isEqualTo(0);
    assertThat(task1.getChildren().length).isEqualTo(2);
    assertThat(task1.getChildren()).contains(stageId2 + 1, stageId3 + 1);

    Task task2 = fakeStageListener2.getProcessedObjects().get(1);
    assertThat(task2.getParents().length).isEqualTo(1);
    assertThat(task2.getParents()).contains(stageId1 + 1);
    assertThat(task2.getChildren().length).isEqualTo(0);

    Task task3 = fakeStageListener2.getProcessedObjects().get(2);
    assertThat(task3.getParents().length).isEqualTo(1);
    assertThat(task3.getParents()).contains(stageId1 + 1);
    assertThat(task3.getChildren().length).isEqualTo(0);
  }
}
