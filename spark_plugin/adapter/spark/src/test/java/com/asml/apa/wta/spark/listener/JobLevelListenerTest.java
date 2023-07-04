package com.asml.apa.wta.spark.listener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
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
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.collection.mutable.ListBuffer;

class JobLevelListenerTest {

  private SparkContext mockedSparkContext;

  private ResourceProfileManager mockedResourceProfileManager;

  private ResourceProfile mockedResource;

  private Map<String, TaskResourceRequest> mapResource;

  private SparkContext mockedSparkContext2;

  private ResourceProfileManager mockedResourceProfileManager2;

  private ResourceProfile mockedResource2;

  private Map<String, TaskResourceRequest> mapResource2;

  private RuntimeConfig fakeConfig1;

  private RuntimeConfig fakeConfig2;

  private TaskLevelListener fakeTaskListener1;

  private StageLevelListener fakeStageListener1;

  private JobLevelListener fakeJobListener1;

  private TaskLevelListener fakeTaskListener2;

  private StageLevelListener fakeStageListener2;

  private JobLevelListener fakeJobListener2;

  private TaskInfo testTaskInfo1;

  private TaskInfo testTaskInfo2;

  private TaskInfo testTaskInfo3;

  private TaskInfo testTaskInfo4;

  private StageInfo testStageInfo1;

  private StageInfo testStageInfo2;

  private StageInfo testStageInfo3;

  private StageInfo testStageInfo4;

  private SparkListenerTaskEnd taskEndEvent1;

  private SparkListenerTaskEnd taskEndEvent2;

  private SparkListenerTaskEnd taskEndEvent3;

  private SparkListenerTaskEnd taskEndEvent4;

  private SparkListenerStageCompleted stageCompleted1;

  private SparkListenerStageCompleted stageCompleted2;

  private SparkListenerStageCompleted stageCompleted3;

  private SparkListenerStageCompleted stageCompleted4;

  private SparkListenerJobEnd jobEndEvent1;

  private SparkListenerJobEnd jobEndEvent2;

  private int taskId1;

  private int taskId2;

  private int taskId3;

  private int taskId4;

  private int stageId1;

  private int stageId2;

  private int stageId3;

  private int stageId4;

  private int jobId1;

  private int jobId2;

  private TaskMetrics mockedMetrics1;

  private TaskMetrics mockedMetrics2;

  private TaskMetrics mockedMetrics3;

  private TaskMetrics mockedMetrics4;

  @BeforeEach
  void setup() {
    mockedSparkContext = mock(SparkContext.class);
    mockedResourceProfileManager = mock(ResourceProfileManager.class);
    mockedResource = mock(ResourceProfile.class);
    mapResource = new HashMap<String, TaskResourceRequest>()
        .$plus(new Tuple2<>("this", new TaskResourceRequest("this", 20)));
    SparkConf conf = new SparkConf().set("spark.app.name", "testApp");
    when(mockedSparkContext.sparkUser()).thenReturn("testUser");
    when(mockedSparkContext.getConf()).thenReturn(conf);
    when(mockedSparkContext.appName()).thenReturn("testApp");
    when(mockedSparkContext.startTime()).thenReturn(5000L);
    when(mockedSparkContext.resourceProfileManager()).thenReturn(mockedResourceProfileManager);
    when(mockedResourceProfileManager.resourceProfileFromId(100)).thenReturn(mockedResource);
    when(mockedResource.taskResources()).thenReturn(mapResource);

    mockedSparkContext2 = mock(SparkContext.class);
    mockedResourceProfileManager2 = mock(ResourceProfileManager.class);
    mockedResource2 = mock(ResourceProfile.class);
    mapResource2 = new HashMap<>();
    when(mockedSparkContext2.sparkUser()).thenReturn("testUser");
    when(mockedSparkContext2.getConf()).thenReturn(conf);
    when(mockedSparkContext2.appName()).thenReturn("testApp");
    when(mockedSparkContext2.startTime()).thenReturn(5000L);
    when(mockedSparkContext2.resourceProfileManager()).thenReturn(mockedResourceProfileManager2);
    when(mockedResourceProfileManager2.resourceProfileFromId(100)).thenReturn(mockedResource2);
    when(mockedResource2.taskResources()).thenReturn(mapResource2);

    fakeConfig1 = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .isStageLevel(false)
        .description("Yer a wizard harry")
        .build();

    fakeTaskListener1 = new TaskLevelListener(mockedSparkContext, fakeConfig1);

    fakeStageListener1 = new StageLevelListener(mockedSparkContext, fakeConfig1);

    fakeJobListener1 = new JobLevelListener(mockedSparkContext, fakeConfig1, fakeTaskListener1, fakeStageListener1);

    SparkDataSource sparkDataSource = mock(SparkDataSource.class);
    when(sparkDataSource.getRuntimeConfig()).thenReturn(mock(RuntimeConfig.class));
    when(sparkDataSource.getTaskLevelListener()).thenReturn(mock(TaskLevelListener.class));
    when(sparkDataSource.getStageLevelListener()).thenReturn(mock(StageLevelListener.class));
    when(sparkDataSource.getJobLevelListener()).thenReturn(mock(JobLevelListener.class));

    fakeConfig2 = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .isStageLevel(true)
        .description("Yer a wizard harry")
        .build();

    fakeTaskListener2 = new TaskLevelListener(mockedSparkContext2, fakeConfig2);

    fakeStageListener2 = new StageLevelListener(mockedSparkContext2, fakeConfig2);

    fakeJobListener2 =
        new JobLevelListener(mockedSparkContext2, fakeConfig2, fakeTaskListener2, fakeStageListener2);

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
  void jobStartAndEndStateIsCorrect() {
    fakeJobListener1.onJobStart(
        new SparkListenerJobStart(jobId1, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    fakeJobListener1.onJobEnd(jobEndEvent1);

    await().atMost(20, SECONDS)
        .until(() -> fakeJobListener1.getProcessedObjects().count() == 1);

    assertThat(fakeJobListener1.getJobSubmitTimes()).isEmpty();

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

    await().atMost(20, SECONDS)
        .until(() -> fakeTaskListener1.getProcessedObjects().count() == 2);

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

    await().atMost(20, SECONDS)
        .until(() -> fakeTaskListener1.getProcessedObjects().count() == 4);

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
