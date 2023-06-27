package com.asml.apa.wta.spark.listener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.stream.Stream;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.util.List;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskLocality;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.collection.mutable.ListBuffer;

class ApplicationLevelListenerTest {

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

  private SparkDataSource sparkDataSource;

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

  int stageId1;

  int stageId2;

  long applicationDateEnd;

  MetricStreamingEngine metricStreamingEngine;

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

    sparkDataSource = mock(SparkDataSource.class);
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
    parents.$plus$eq(stageId1);

    testStageInfo1 = new StageInfo(
        stageId1, 0, "test", 50, null, new ListBuffer<>(), "None", mockedMetrics1, null, null, 100);
    testStageInfo2 = new StageInfo(stageId2, 0, "test", 50, null, parents, "None", mockedMetrics1, null, null, 100);

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

    applicationDateEnd = mockedSparkContext.startTime() + 1000L;
    applicationEndObj = new SparkListenerApplicationEnd(applicationDateEnd);

    metricStreamingEngine = mock(MetricStreamingEngine.class);
    when(metricStreamingEngine.collectResourceInformation()).thenReturn(List.of());
  }

  @Test
  void workloadBuiltWithDefaultMetricValues() {
    assertThat(fakeApplicationListener1.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener1.getWorkload() != null);
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener1.getWorkload();
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);
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
    assertThat(fakeApplicationListener1.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener1.getWorkload() != null);
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener1.getWorkload();
    assertThat(workload.getDomain()).isEqualTo(fakeConfig1.getDomain());
    assertThat(workload.getAuthors()).isEqualTo(fakeConfig1.getAuthors());
    assertThat(workload.getWorkloadDescription()).isEqualTo(fakeConfig1.getDescription());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getDateStart()).isEqualTo(sutStartTime);
    assertThat(workload.getDateEnd()).isEqualTo(applicationDateEnd);
  }

  @Test
  void workloadCountMetricsCollected() {
    assertThat(fakeApplicationListener1.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener1.getWorkload() != null);
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener1.getWorkload();
    assertThat(workload.getTotalWorkflows()).isEqualTo(0);
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getNumUsers()).isEqualTo(-1L);
    assertThat(workload.getNumGroups()).isEqualTo(-1L);
    assertThat(workload.getNumSites()).isEqualTo(-1L);
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

    assertThat(fakeApplicationListener1.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener1.getWorkload() != null);

    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener1.getWorkload();
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(133.33333333333334);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1.0);
  }

  @Test
  void stageWithoutTaskTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener1.onJobStart(jobStart);
    fakeStageListener1.onJobStart(jobStart);
    fakeStageListener1.onStageCompleted(stageCompleted2);

    assertThat(fakeApplicationListener1.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener1.getWorkload() != null);
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener1.getWorkload();
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
    assertThat(fakeApplicationListener1.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS)
        .until(() -> fakeApplicationListener1.getProcessedObjects().count() == 0);

    fakeTaskListener1.onJobStart(jobStart2);
    fakeStageListener1.onJobStart(jobStart2);
    fakeTaskListener1.onTaskEnd(taskEndEvent3);
    fakeTaskListener1.onTaskEnd(taskEndEvent4);
    fakeStageListener1.onStageCompleted(stageCompleted2);
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);
    fakeApplicationListener1.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener1.getProcessedObjects().count()).isEqualTo(0);
  }

  @Test
  void writeTrace() {
    WtaWriter writer = mock(WtaWriter.class);
    MetricStreamingEngine streamingEngine = mock(MetricStreamingEngine.class);
    ResourceState resourceState = mock(ResourceState.class);
    Resource resource = Resource.builder().os("Hannah Montana Linux").build();
    when(streamingEngine.collectResourceInformation())
        .thenReturn(List.of(new ResourceAndStateWrapper(resource, new Stream<>(resourceState))));
    ApplicationLevelListener listener = new ApplicationLevelListener(
        mockedSparkContext2,
        fakeConfig1,
        fakeTaskListener1,
        fakeStageListener1,
        fakeJobListener1,
        sparkDataSource,
        streamingEngine,
        writer);
    try (MockedStatic<Stream> streamMock = mockStatic(Stream.class)) {
      listener.writeTrace();
      verify(sparkDataSource).awaitAndShutdownThreadPool(anyInt());
      ArgumentCaptor<Stream<Resource>> resourceArgumentCaptor = ArgumentCaptor.forClass(Stream.class);
      verify(writer).write(eq(Resource.class), resourceArgumentCaptor.capture());
      assertThat(resourceArgumentCaptor.getValue().head().getOs()).isEqualTo("Hannah Montana Linux");
      streamMock.verify(Stream::deleteAllSerializedFiles);
    }
  }
}
