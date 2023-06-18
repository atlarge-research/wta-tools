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

  SparkListenerTaskEnd taskEndEvent1;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerTaskEnd taskEndEvent3;

  SparkListenerTaskEnd taskEndEvent4;

  SparkListenerStageCompleted stageCompleted1;

  SparkListenerStageCompleted stageCompleted2;

  @BeforeEach
  void setup() {
    testTaskInfo1 = new TaskInfo(0, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo3 = new TaskInfo(2, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo4 = new TaskInfo(3, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    ListBuffer<Object> parents = new ListBuffer<>();
    TaskMetrics mockedMetrics1 = mock(TaskMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics = mock(ShuffleWriteMetrics.class);
    when(mockedMetrics1.executorRunTime()).thenReturn(100L);
    when(mockedMetrics1.diskBytesSpilled()).thenReturn(100L);
    when(mockedMetrics1.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics);
    when(mockedShuffleMetrics.bytesWritten()).thenReturn(100L);

    TaskMetrics mockedMetrics2 = mock(TaskMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics2 = mock(ShuffleWriteMetrics.class);
    when(mockedMetrics2.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics2.diskBytesSpilled()).thenReturn(-1L);
    when(mockedMetrics2.executorRunTime()).thenReturn(-1L);
    when(mockedMetrics2.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics2);
    when(mockedShuffleMetrics2.bytesWritten()).thenReturn(-1L);

    TaskMetrics mockedMetrics3 = mock(TaskMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics3 = mock(ShuffleWriteMetrics.class);
    when(mockedMetrics3.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics3.diskBytesSpilled()).thenReturn(0L);
    when(mockedMetrics3.executorRunTime()).thenReturn(-1L);
    when(mockedMetrics3.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics2);
    when(mockedShuffleMetrics3.bytesWritten()).thenReturn(0L);

    testStageInfo1 =
        new StageInfo(2, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics1, null, null, 100);
    parents.$plus$eq(testStageInfo1.stageId());
    testStageInfo2 = new StageInfo(10, 0, "test", 50, null, parents, "None", mockedMetrics1, null, null, 100);
    taskEndEvent1 = new SparkListenerTaskEnd(
        2, 1, "testTaskType", null, testTaskInfo1, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent2 = new SparkListenerTaskEnd(
        2, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent3 = new SparkListenerTaskEnd(
        10, 1, "testTaskType", null, testTaskInfo3, new ExecutorMetrics(), mockedMetrics2);
    taskEndEvent4 = new SparkListenerTaskEnd(
        10, 1, "testTaskType", null, testTaskInfo4, new ExecutorMetrics(), mockedMetrics3);

    stageCompleted1 = new SparkListenerStageCompleted(testStageInfo1);
    stageCompleted2 = new SparkListenerStageCompleted(testStageInfo2);
    applicationEndObj = new SparkListenerApplicationEnd(mockedSparkContext.startTime() + 1000L);
  }

  @Test
  void doingNothingTest() {
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
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
  void uninitializedTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeStageListener.onStageCompleted(stageCompleted2);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(20);
    assertThat(workload.getMaxResourceTask()).isEqualTo(20);
    assertThat(workload.getCovResourceTask()).isEqualTo(0);
    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getStdMemory()).isEqualTo(-1);
    assertThat(workload.getMedianMemory()).isEqualTo(-1);
    assertThat(workload.getMinMemory()).isEqualTo(-1);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(20);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1);
  }

//  @Test
//  void zeroMeanTest() {
//    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
//    stageBuffer.$plus$eq(testStageInfo2);
//    SparkListenerJobStart jobStart = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());
//    fakeTaskListener.onJobStart(jobStart);
//    fakeStageListener.onJobStart(jobStart);
//    fakeTaskListener.onTaskEnd(taskEndEvent4);
//    fakeStageListener.onStageCompleted(stageCompleted2);
//    fakeApplicationListener.onApplicationEnd(applicationEndObj);
//    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
//    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
//    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(20);
//    assertThat(workload.getMaxResourceTask()).isEqualTo(20);
//    assertThat(workload.getCovResourceTask()).isEqualTo(0);
//    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(-1);
//    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(0);
//    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(0);
//    assertThat(workload.getStdMemory()).isEqualTo(-1);
//    assertThat(workload.getMedianMemory()).isEqualTo(-1);
//    assertThat(workload.getMinMemory()).isEqualTo(-1);
//    assertThat(workload.getTotalTasks()).isEqualTo(0);
//    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
//    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
//    assertThat(workload.getMeanResourceTask()).isEqualTo(20);
//    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1);
//    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(0);
//    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1);
//  }

  @Test
  void stageWithoutTaskTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeStageListener.onStageCompleted(stageCompleted2);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
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
  void applicationEndTwoTimesTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeStageListener.onStageCompleted(stageCompleted2);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(20);
    assertThat(workload.getMaxResourceTask()).isEqualTo(20);
    assertThat(workload.getCovResourceTask()).isEqualTo(0);
    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(-1);
    assertThat(workload.getStdMemory()).isEqualTo(-1);
    assertThat(workload.getMedianMemory()).isEqualTo(-1);
    assertThat(workload.getMinMemory()).isEqualTo(-1);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(20);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1);
  }

//  @Test
//  void parentChildrenAggregationTest() {
//    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
//    stageBuffer.$plus$eq(testStageInfo1);
//    stageBuffer.$plus$eq(testStageInfo2);
//    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
//    fakeTaskListener.onJobStart(jobStart);
//    fakeStageListener.onJobStart(jobStart);
//    fakeTaskListener.onTaskEnd(taskEndEvent1);
//    fakeTaskListener.onTaskEnd(taskEndEvent2);
//    fakeStageListener.onStageCompleted(stageCompleted1);
//    fakeTaskListener.onTaskEnd(taskEndEvent3);
//    fakeTaskListener.onTaskEnd(taskEndEvent4);
//    fakeStageListener.onStageCompleted(stageCompleted2);
//    fakeApplicationListener.onApplicationEnd(applicationEndObj);
//    Task task = fakeTaskListener.getProcessedObjects().get(0);
//    assertThat(task.getParents().length).isEqualTo(0);
//    assertThat(task.getChildren().length).isEqualTo(2);
//    assertThat(task.getChildren()).contains(3L, 4L);
//    task = fakeTaskListener.getProcessedObjects().get(1);
//    assertThat(task.getParents().length).isEqualTo(0);
//    assertThat(task.getChildren().length).isEqualTo(2);
//    assertThat(task.getChildren()).contains(3L, 4L);
//    task = fakeTaskListener.getProcessedObjects().get(2);
//    assertThat(task.getParents().length).isEqualTo(2);
//    assertThat(task.getParents()).contains(1L, 2L);
//    assertThat(task.getChildren().length).isEqualTo(0);
//    task = fakeTaskListener.getProcessedObjects().get(3);
//    assertThat(task.getParents().length).isEqualTo(2);
//    assertThat(task.getParents()).contains(1L, 2L);
//    assertThat(task.getChildren().length).isEqualTo(0);
//    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
//    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
//    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(20);
//    assertThat(workload.getMaxResourceTask()).isEqualTo(20);
//    assertThat(workload.getCovResourceTask()).isEqualTo(0);
//    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(0.7071067811865474);
//    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(0);
//    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(0);
//    assertThat(workload.getStdMemory()).isEqualTo(-1);
//    assertThat(workload.getMedianMemory()).isEqualTo(-1);
//    assertThat(workload.getMinMemory()).isEqualTo(-1);
//    assertThat(workload.getTotalTasks()).isEqualTo(0);
//    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
//    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
//    assertThat(workload.getMeanResourceTask()).isEqualTo(20);
//    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
//    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(133.33333333333334);
//    assertThat(workload.getTotalResourceSeconds()).isEqualTo(4000);
//  }

  @Test
  void parentChildrenAggregationAltTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener2.onJobStart(jobStart);
    fakeStageListener2.onJobStart(jobStart);
    fakeTaskListener2.onTaskEnd(taskEndEvent1);
    fakeTaskListener2.onTaskEnd(taskEndEvent2);
    fakeStageListener2.onStageCompleted(stageCompleted1);
    fakeTaskListener2.onTaskEnd(taskEndEvent3);
    fakeTaskListener2.onTaskEnd(taskEndEvent4);
    fakeStageListener2.onStageCompleted(stageCompleted2);
    fakeApplicationListener2.onApplicationEnd(applicationEndObj);
    Workload workload = fakeApplicationListener2.getProcessedObjects().get(0);
    assertThat(fakeApplicationListener2.getProcessedObjects().size()).isEqualTo(1);
    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(-1);
    assertThat(workload.getMaxResourceTask()).isEqualTo(-1);
    assertThat(workload.getCovResourceTask()).isEqualTo(-1);
    assertThat(workload.getMeanResourceTask()).isEqualTo(-1);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1);
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

    assertThat(fakeApplicationListener.getProcessedObjects().size()).isEqualTo(1);
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
}
