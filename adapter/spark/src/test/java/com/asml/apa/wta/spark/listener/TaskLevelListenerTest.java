package com.asml.apa.wta.spark.listener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskLocality;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.collection.mutable.ListBuffer;

class TaskLevelListenerTest {

  protected SparkContext mockedSparkContext;

  protected ResourceProfileManager mockedResourceProfileManager;

  protected ResourceProfile mockedResource;

  protected Map<String, TaskResourceRequest> mapResource;

  protected SparkContext mockedSparkContext2;

  protected ResourceProfileManager mockedResourceProfileManager2;

  protected ResourceProfile mockedResource2;

  protected Map<String, TaskResourceRequest> mapResource2;

  protected RuntimeConfig fakeConfig1;

  protected RuntimeConfig fakeConfig2;

  protected TaskLevelListener fakeTaskListener1;
  protected StageLevelListener fakeStageListener1;
  protected JobLevelListener fakeJobListener1;
  protected ApplicationLevelListener fakeApplicationListener1;

  protected TaskLevelListener fakeTaskListener2;
  protected StageLevelListener fakeStageListener2;
  protected JobLevelListener fakeJobListener2;
  protected ApplicationLevelListener fakeApplicationListener2;

  TaskInfo testTaskInfo1;

  TaskInfo testTaskInfo2;

  StageInfo testStageInfo;

  SparkListenerTaskEnd taskEndEvent;

  SparkListenerTaskEnd taskEndEvent2;

  SparkListenerStageCompleted stageCompleted;

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
    fakeStageListener1 = new StageLevelListener(mockedSparkContext, fakeConfig1);

    fakeTaskListener1 = new TaskLevelListener(mockedSparkContext, fakeConfig1);

    fakeStageListener1 = new StageLevelListener(mockedSparkContext, fakeConfig1);

    fakeJobListener1 = new JobLevelListener(mockedSparkContext, fakeConfig1, fakeTaskListener1, fakeStageListener1);

    SparkDataSource sparkDataSource = mock(SparkDataSource.class);
    when(sparkDataSource.getRuntimeConfig()).thenReturn(mock(RuntimeConfig.class));
    when(sparkDataSource.getTaskLevelListener()).thenReturn(mock(TaskLevelListener.class));
    when(sparkDataSource.getStageLevelListener()).thenReturn(mock(StageLevelListener.class));
    when(sparkDataSource.getJobLevelListener()).thenReturn(mock(JobLevelListener.class));

    fakeApplicationListener1 = new ApplicationLevelListener(
        mockedSparkContext,
        fakeConfig1,
        fakeTaskListener1,
        fakeStageListener1,
        fakeJobListener1,
        sparkDataSource,
        mock(MetricStreamingEngine.class),
        mock(WtaWriter.class));

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

    fakeApplicationListener2 = new ApplicationLevelListener(
        mockedSparkContext2,
        fakeConfig2,
        fakeTaskListener2,
        fakeStageListener2,
        fakeJobListener2,
        sparkDataSource,
        mock(MetricStreamingEngine.class),
        mock(WtaWriter.class));
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
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).size()).isEqualTo(1);
    assertThat(fakeTaskListener1.getStageToTasks().get(3L).get(0).getId()).isEqualTo(1);
    assertThat(fakeTaskListener1.getTaskToStage().size()).isEqualTo(1);
    assertThat(fakeTaskListener1.getTaskToStage()).containsEntry(1L, 3L);
    fakeTaskListener1.onTaskEnd(taskEndEvent2);
    assertThat(fakeTaskListener1.getStageToTasks().size()).isEqualTo(1);
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

    await().atMost(20, SECONDS)
        .until(() -> fakeTaskListener1.getProcessedObjects().count() == 1);

    Task curTask = fakeTaskListener1.getProcessedObjects().head();
    assertEquals(1, curTask.getId());
    assertEquals("testTaskType", curTask.getType());
    assertEquals(50L, curTask.getTsSubmit());
    assertEquals(100L, curTask.getRuntime());
    assertEquals(1L, curTask.getWorkflowId());
    assertEquals(Math.abs("testUser".hashCode()), curTask.getUserId());
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
