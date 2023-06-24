package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskLocality;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

class JobLevelListenerTest extends BaseLevelListenerTest {

  TaskInfo testTaskInfo1;

  TaskInfo testTaskInfo2;

  TaskInfo testTaskInfo3;

  TaskInfo testTaskInfo4;

  StageInfo testStageInfo1;

  StageInfo testStageInfo2;

  StageInfo testStageInfo3;

  StageInfo testStageInfo4;

  SparkListenerTaskEnd taskEndEvent1;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerTaskEnd taskEndEvent3;

  SparkListenerTaskEnd taskEndEvent4;

  SparkListenerStageCompleted stageCompleted1;

  SparkListenerStageCompleted stageCompleted2;

  SparkListenerStageCompleted stageCompleted3;

  SparkListenerStageCompleted stageCompleted4;

  SparkListenerJobEnd jobEndEvent1;

  SparkListenerJobEnd jobEndEvent2;

  int taskId1;

  int taskId2;

  int taskId3;

  int taskId4;

  int stageId1;

  int stageId2;

  int stageId3;

  int stageId4;

  int jobId1;

  int jobId2;

  TaskMetrics mockedMetrics1;

  TaskMetrics mockedMetrics2;

  TaskMetrics mockedMetrics3;

  TaskMetrics mockedMetrics4;

  @BeforeEach
  void setup() {
    taskId1 = 0;
    taskId2 = 1;
    taskId3 = 2;
    taskId4 = 3;
    testTaskInfo1 = new TaskInfo(taskId1, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo2 = new TaskInfo(taskId2, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo3 = new TaskInfo(taskId3, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);
    testTaskInfo4 = new TaskInfo(taskId4, 0, 1, 50L, "testExecutor", "local", TaskLocality.NODE_LOCAL(), false);

    ShuffleWriteMetrics mockedShuffleMetrics1 = mock(ShuffleWriteMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics2 = mock(ShuffleWriteMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics3 = mock(ShuffleWriteMetrics.class);
    ShuffleWriteMetrics mockedShuffleMetrics4 = mock(ShuffleWriteMetrics.class);
    when(mockedShuffleMetrics1.bytesWritten()).thenReturn(100L);
    when(mockedShuffleMetrics2.bytesWritten()).thenReturn(-1L);
    when(mockedShuffleMetrics3.bytesWritten()).thenReturn(0L);
    when(mockedShuffleMetrics4.bytesWritten()).thenReturn(50L);

    mockedMetrics1 = mock(TaskMetrics.class);
    when(mockedMetrics1.peakExecutionMemory()).thenReturn(100L);
    when(mockedMetrics1.executorRunTime()).thenReturn(100L);
    when(mockedMetrics1.diskBytesSpilled()).thenReturn(100L);
    when(mockedMetrics1.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics1);

    mockedMetrics2 = mock(TaskMetrics.class);
    when(mockedMetrics2.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics2.diskBytesSpilled()).thenReturn(-1L);
    when(mockedMetrics2.executorRunTime()).thenReturn(-1L);
    when(mockedMetrics2.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics2);

    mockedMetrics3 = mock(TaskMetrics.class);
    when(mockedMetrics3.peakExecutionMemory()).thenReturn(-1L);
    when(mockedMetrics3.diskBytesSpilled()).thenReturn(0L);
    when(mockedMetrics3.executorRunTime()).thenReturn(-1L);
    when(mockedMetrics3.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics3);

    mockedMetrics4 = mock(TaskMetrics.class);
    when(mockedMetrics4.peakExecutionMemory()).thenReturn(30L);
    when(mockedMetrics4.diskBytesSpilled()).thenReturn(20L);
    when(mockedMetrics4.executorRunTime()).thenReturn(80L);
    when(mockedMetrics4.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics4);

    stageId1 = 2;
    stageId2 = 10;
    stageId3 = 14;
    stageId4 = 19;

    taskEndEvent1 = new SparkListenerTaskEnd(
        stageId1, 1, "testTaskType", null, testTaskInfo1, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent2 = new SparkListenerTaskEnd(
        stageId1, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent3 = new SparkListenerTaskEnd(
        stageId2, 1, "testTaskType", null, testTaskInfo3, new ExecutorMetrics(), mockedMetrics2);
    taskEndEvent4 = new SparkListenerTaskEnd(
        stageId2, 1, "testTaskType", null, testTaskInfo4, new ExecutorMetrics(), mockedMetrics3);

    // stage 2 and stage 3 both have stage 1 as parent
    // stage 4 has stage 2 and stage 3 as parent
    ListBuffer<Object> parents1 = new ListBuffer<>();
    ListBuffer<Object> parents2 = new ListBuffer<>();
    parents1.$plus$eq(stageId1);
    parents2.$plus$eq(stageId2);
    parents2.$plus$eq(stageId3);

    testStageInfo1 = new StageInfo(
        stageId1, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics1, null, null, 100);
    testStageInfo2 =
        new StageInfo(stageId2, 0, "test", 50, null, parents1, "None", mockedMetrics1, null, null, 100);
    testStageInfo3 =
        new StageInfo(stageId3, 0, "test", 50, null, parents1, "None", mockedMetrics1, null, null, 100);
    testStageInfo4 =
        new StageInfo(stageId4, 0, "test", 50, null, parents2, "None", mockedMetrics1, null, null, 100);

    stageCompleted1 = new SparkListenerStageCompleted(testStageInfo1);
    stageCompleted2 = new SparkListenerStageCompleted(testStageInfo2);
    stageCompleted3 = new SparkListenerStageCompleted(testStageInfo3);
    stageCompleted4 = new SparkListenerStageCompleted(testStageInfo4);
    jobId1 = 0;
    jobId2 = 1;
    jobEndEvent1 = new SparkListenerJobEnd(jobId1, 1, new JobFailed(new RuntimeException("test")));
    jobEndEvent2 = new SparkListenerJobEnd(jobId2, 1, new JobFailed(new RuntimeException("test")));
  }

  @Test
  void recordsTheTimeWhenJobIsSubmittedInMap() {
    fakeJobListener1.onJobStart(
        new SparkListenerJobStart(jobId1, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    assertThat(fakeJobListener1.getJobSubmitTimes()).containsEntry((long) (jobId1 + 1), 40L);
  }

  @Test
  void jobStartAndEndStateIsCorrect() throws InterruptedException {
    fakeJobListener1.onJobStart(
        new SparkListenerJobStart(jobId1, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    fakeJobListener1.onJobEnd(jobEndEvent1);

    AbstractListener.getThreadPool().awaitTermination(1000, TimeUnit.MILLISECONDS);

    assertThat(fakeJobListener1.getJobSubmitTimes()).isEmpty();
    assertThat(fakeJobListener1.getProcessedObjects().count()).isEqualTo(1);

    Workflow fakeJobListenerWorkflow =
        fakeJobListener1.getProcessedObjects().head();
    assertThat(fakeJobListenerWorkflow.getId()).isEqualTo(jobId1 + 1);
    assertThat(fakeJobListenerWorkflow.getTsSubmit()).isEqualTo(40L);
    assertThat(fakeJobListenerWorkflow.getScheduler()).isEqualTo("FIFO");
    assertThat(fakeJobListenerWorkflow.getApplicationName()).isEqualTo("testApp");

    assertThat(fakeJobListenerWorkflow.getNfrs()).isEmpty();
    assertThat(fakeJobListenerWorkflow.getApplicationField()).isEqualTo("ETL");
  }

  @Test
  void parentChildrenAggregatedForTaskLevelMetrics() {
    // task 1, task 2 have parent child relation with task3, task4 in stage 1, job 1
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart1 = new SparkListenerJobStart(jobId1, 2L, stageBuffer.toList(), new Properties());

    fakeTaskListener1.onJobStart(jobStart1);
    fakeStageListener1.onJobStart(jobStart1);
    fakeJobListener1.onJobStart(jobStart1);
    fakeTaskListener1.onTaskEnd(taskEndEvent1);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    fakeStageListener1.onStageCompleted(stageCompleted1);
    fakeTaskListener1.onTaskEnd(taskEndEvent3);
    fakeTaskListener1.onTaskEnd(taskEndEvent4);
    fakeStageListener1.onStageCompleted(stageCompleted2);
    fakeJobListener1.onJobEnd(jobEndEvent1);
    assertThat(fakeTaskListener1.getProcessedObjects().count()).isEqualTo(4);

    Task task1 = fakeTaskListener1.getProcessedObjects().head();
    assertThat(task1.getParents().length).isEqualTo(0);
    assertThat(task1.getChildren().length).isEqualTo(2);
    assertThat(task1.getChildren()).contains(taskId3 + 1, taskId4 + 1);

    Task task2 = fakeTaskListener1.getProcessedObjects().drop(1).head();
    assertThat(task2.getParents().length).isEqualTo(0);
    assertThat(task2.getChildren().length).isEqualTo(2);
    assertThat(task2.getChildren()).contains(taskId3 + 1, taskId4 + 1);

    Task task3 = fakeTaskListener1.getProcessedObjects().drop(2).head();
    assertThat(task3.getParents().length).isEqualTo(2);
    assertThat(task3.getParents()).contains(taskId1 + 1, taskId2 + 1);
    assertThat(task3.getChildren().length).isEqualTo(0);

    Task task4 = fakeTaskListener1.getProcessedObjects().drop(3).head();
    assertThat(task4.getParents().length).isEqualTo(2);
    assertThat(task4.getParents()).contains(taskId1 + 1, taskId2 + 1);
    assertThat(task4.getChildren().length).isEqualTo(0);
  }

  @Test
  void parentChildrenAggregationForTasksHoldsAcrossMultipleJobs() {
    // task 1 and task 2 have parent child relation in job 1
    // task 3 and task 4 have parent child relation in job 1
    ListBuffer<StageInfo> stageBuffer1 = new ListBuffer<>();
    ListBuffer<StageInfo> stageBuffer2 = new ListBuffer<>();
    stageBuffer1.$plus$eq(testStageInfo1);
    stageBuffer1.$plus$eq(testStageInfo2);
    stageBuffer2.$plus$eq(testStageInfo3);
    stageBuffer2.$plus$eq(testStageInfo4);
    SparkListenerJobStart jobStart1 =
        new SparkListenerJobStart(jobId1, 2L, stageBuffer1.toList(), new Properties());
    SparkListenerJobStart jobStart2 =
        new SparkListenerJobStart(jobId2, 2L, stageBuffer2.toList(), new Properties());

    taskEndEvent1 = new SparkListenerTaskEnd(
        stageId1, 1, "testTaskType", null, testTaskInfo1, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent2 = new SparkListenerTaskEnd(
        stageId2, 1, "testTaskType", null, testTaskInfo2, new ExecutorMetrics(), mockedMetrics1);
    taskEndEvent3 = new SparkListenerTaskEnd(
        stageId3, 1, "testTaskType", null, testTaskInfo3, new ExecutorMetrics(), mockedMetrics2);
    taskEndEvent4 = new SparkListenerTaskEnd(
        stageId4, 1, "testTaskType", null, testTaskInfo4, new ExecutorMetrics(), mockedMetrics3);

    fakeTaskListener1.onJobStart(jobStart1);
    fakeStageListener1.onJobStart(jobStart1);
    fakeJobListener1.onJobStart(jobStart1);
    fakeTaskListener1.onTaskEnd(taskEndEvent1);
    fakeStageListener1.onStageCompleted(stageCompleted1);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    fakeStageListener1.onStageCompleted(stageCompleted2);
    fakeJobListener1.onJobEnd(jobEndEvent1);
    assertThat(fakeTaskListener1.getProcessedObjects().count()).isEqualTo(2);

    Task task1 = fakeTaskListener1.getProcessedObjects().head();
    assertThat(task1.getParents().length).isEqualTo(0);
    assertThat(task1.getChildren().length).isEqualTo(1);
    assertThat(task1.getChildren()).contains(taskId2 + 1);

    Task task2 = fakeTaskListener1.getProcessedObjects().drop(1).head();
    assertThat(task2.getParents().length).isEqualTo(1);
    assertThat(task2.getParents()).contains(taskId1 + 1);
    assertThat(task2.getChildren().length).isEqualTo(0);

    fakeTaskListener1.onJobStart(jobStart2);
    fakeStageListener1.onJobStart(jobStart2);
    fakeJobListener1.onJobStart(jobStart2);
    fakeTaskListener1.onTaskEnd(taskEndEvent3);
    fakeStageListener1.onStageCompleted(stageCompleted3);
    fakeTaskListener1.onTaskEnd(taskEndEvent4);
    fakeStageListener1.onStageCompleted(stageCompleted4);
    fakeJobListener1.onJobEnd(jobEndEvent2);
    assertThat(fakeTaskListener1.getProcessedObjects().count()).isEqualTo(4);

    Task task3 = fakeTaskListener1.getProcessedObjects().drop(2).head();
    assertThat(task3.getParents().length).isEqualTo(0);
    assertThat(task3.getChildren().length).isEqualTo(1);
    assertThat(task3.getChildren()).contains(taskId4 + 1);

    Task task4 = fakeTaskListener1.getProcessedObjects().drop(3).head();
    assertThat(task4.getParents().length).isEqualTo(1);
    assertThat(task4.getParents()).contains(taskId3 + 1);
    assertThat(task4.getChildren().length).isEqualTo(0);
  }

  @Test
  void parentChildrenAggregatedForStageLevelMetrics() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    stageBuffer.$plus$eq(testStageInfo3);
    stageBuffer.$plus$eq(testStageInfo4);
    SparkListenerJobStart jobStart1 = new SparkListenerJobStart(jobId1, 2L, stageBuffer.toList(), new Properties());

    fakeStageListener2.onJobStart(jobStart1);
    fakeJobListener2.onJobStart(jobStart1);
    fakeStageListener2.onStageCompleted(stageCompleted1);
    fakeStageListener2.onStageCompleted(stageCompleted2);
    fakeStageListener2.onStageCompleted(stageCompleted3);
    fakeStageListener2.onStageCompleted(stageCompleted4);
    fakeJobListener2.onJobEnd(jobEndEvent1);

    // stage 2 and stage 3 both have stage 1 as parent
    // stage 4 has stage 2 and stage 3 as parent
    assertThat(fakeStageListener2.getProcessedObjects().count()).isEqualTo(4);

    Task stage1 = fakeStageListener2.getProcessedObjects().head();
    assertThat(stage1.getParents().length).isEqualTo(0);
    assertThat(stage1.getChildren().length).isEqualTo(2);
    assertThat(stage1.getChildren()).contains(stageId2 + 1, stageId3 + 1);

    Task stage2 = fakeStageListener2.getProcessedObjects().drop(1).head();
    assertThat(stage2.getParents().length).isEqualTo(1);
    assertThat(stage2.getParents()).contains(stageId1 + 1);
    assertThat(stage2.getChildren().length).isEqualTo(1);
    assertThat(stage2.getChildren()).contains(stageId4 + 1);

    Task stage3 = fakeStageListener2.getProcessedObjects().drop(2).head();
    assertThat(stage3.getParents().length).isEqualTo(1);
    assertThat(stage3.getParents()).contains(stageId1 + 1);
    assertThat(stage3.getChildren().length).isEqualTo(1);
    assertThat(stage3.getChildren()).contains(stageId4 + 1);

    Task stage4 = fakeStageListener2.getProcessedObjects().drop(3).head();
    assertThat(stage4.getParents().length).isEqualTo(2);
    assertThat(stage4.getParents()).contains(stageId2 + 1, stageId3 + 1);
    assertThat(stage4.getChildren().length).isEqualTo(0);
  }

  @Test
  void parentChildrenAggregationForStagesHoldsAcrossMultipleJobs() {
    // stage 1 and stage 2 have parent child relation in job 1
    // stage 3 and stage 4 have parent child relation in job 2
    ListBuffer<Object> parents1 = new ListBuffer<>();
    ListBuffer<Object> parents2 = new ListBuffer<>();
    parents1.$plus$eq(stageId1);
    parents2.$plus$eq(stageId3);

    testStageInfo1 = new StageInfo(
        stageId1, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics1, null, null, 100);
    testStageInfo2 =
        new StageInfo(stageId2, 0, "test", 50, null, parents1, "None", mockedMetrics1, null, null, 100);
    testStageInfo3 = new StageInfo(
        stageId3, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics1, null, null, 100);
    testStageInfo4 =
        new StageInfo(stageId4, 0, "test", 50, null, parents2, "None", mockedMetrics1, null, null, 100);

    stageCompleted1 = new SparkListenerStageCompleted(testStageInfo1);
    stageCompleted2 = new SparkListenerStageCompleted(testStageInfo2);
    stageCompleted3 = new SparkListenerStageCompleted(testStageInfo3);
    stageCompleted4 = new SparkListenerStageCompleted(testStageInfo4);

    ListBuffer<StageInfo> stageBuffer1 = new ListBuffer<>();
    ListBuffer<StageInfo> stageBuffer2 = new ListBuffer<>();
    stageBuffer1.$plus$eq(testStageInfo1);
    stageBuffer1.$plus$eq(testStageInfo2);
    stageBuffer2.$plus$eq(testStageInfo3);
    stageBuffer2.$plus$eq(testStageInfo4);
    SparkListenerJobStart jobStart1 =
        new SparkListenerJobStart(jobId1, 2L, stageBuffer1.toList(), new Properties());
    SparkListenerJobStart jobStart2 =
        new SparkListenerJobStart(jobId2, 2L, stageBuffer2.toList(), new Properties());

    fakeStageListener2.onJobStart(jobStart1);
    fakeJobListener2.onJobStart(jobStart1);
    fakeStageListener2.onStageCompleted(stageCompleted1);
    fakeStageListener2.onStageCompleted(stageCompleted2);
    fakeJobListener2.onJobEnd(jobEndEvent1);
    assertThat(fakeStageListener2.getProcessedObjects().count()).isEqualTo(2);

    Task stage1 = fakeStageListener2.getProcessedObjects().head();
    assertThat(stage1.getParents().length).isEqualTo(0);
    assertThat(stage1.getChildren().length).isEqualTo(1);
    assertThat(stage1.getChildren()).contains(stageId2 + 1);

    Task stage2 = fakeStageListener2.getProcessedObjects().drop(1).head();
    assertThat(stage2.getParents().length).isEqualTo(1);
    assertThat(stage2.getParents()).contains(stageId1 + 1);
    assertThat(stage2.getChildren().length).isEqualTo(0);

    fakeStageListener2.onJobStart(jobStart2);
    fakeJobListener2.onJobStart(jobStart2);
    fakeStageListener2.onStageCompleted(stageCompleted3);
    fakeStageListener2.onStageCompleted(stageCompleted4);
    fakeJobListener2.onJobEnd(jobEndEvent2);
    assertThat(fakeStageListener2.getProcessedObjects().count()).isEqualTo(4);

    Task stage3 = fakeStageListener2.getProcessedObjects().drop(2).head();
    assertThat(stage3.getParents().length).isEqualTo(0);
    assertThat(stage3.getChildren().length).isEqualTo(1);
    assertThat(stage3.getChildren()).contains(stageId4 + 1);

    Task stage4 = fakeStageListener2.getProcessedObjects().drop(3).head();
    assertThat(stage4.getParents().length).isEqualTo(1);
    assertThat(stage4.getParents()).contains(stageId3 + 1);
    assertThat(stage4.getChildren().length).isEqualTo(0);
  }
}
