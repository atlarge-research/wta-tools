package com.asml.apa.wta.spark.listener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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

  private SparkContext mockedSparkContext;

  private ResourceProfileManager mockedResourceProfileManager;

  private ResourceProfile mockedResource;

  private Map<String, TaskResourceRequest> mapResource;

  private SparkContext mockedSparkContext2;

  private ResourceProfileManager mockedResourceProfileManager2;

  private ResourceProfile mockedResource2;

  private Map<String, TaskResourceRequest> mapResource2;

  private RuntimeConfig fakeConfig;

  private TaskLevelListener fakeTaskListener;

  private StageLevelListener fakeStageListener;

  private JobLevelListener fakeJobListener;

  private ApplicationLevelListener fakeApplicationListener;

  private SparkDataSource sparkDataSource;

  private SparkListenerApplicationEnd applicationEndObj;

  private TaskInfo testTaskInfo1;

  private TaskInfo testTaskInfo2;

  private TaskInfo testTaskInfo3;

  private TaskInfo testTaskInfo4;

  private StageInfo testStageInfo1;

  private StageInfo testStageInfo2;

  private SparkListenerTaskEnd taskEndEvent1;

  private SparkListenerTaskEnd taskEndEvent2;

  private SparkListenerTaskEnd taskEndEvent3;

  SparkListenerTaskEnd taskEndEvent4;

  private SparkListenerStageCompleted stageCompleted1;

  private SparkListenerStageCompleted stageCompleted2;

  private int stageId1;

  private int stageId2;

  private long applicationDateEnd;

  private MetricStreamingEngine metricStreamingEngine;

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

    fakeConfig = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .isStageLevel(false)
        .description("Yer a wizard harry")
        .aggregateMetrics(true)
        .build();

    fakeTaskListener = new TaskLevelListener(mockedSparkContext, fakeConfig);

    fakeStageListener = new StageLevelListener(mockedSparkContext, fakeConfig);

    fakeJobListener = new JobLevelListener(mockedSparkContext, fakeConfig, fakeTaskListener, fakeStageListener);

    sparkDataSource = mock(SparkDataSource.class);
    when(sparkDataSource.getRuntimeConfig()).thenReturn(mock(RuntimeConfig.class));
    when(sparkDataSource.getTaskLevelListener()).thenReturn(mock(TaskLevelListener.class));
    when(sparkDataSource.getStageLevelListener()).thenReturn(mock(StageLevelListener.class));
    when(sparkDataSource.getJobLevelListener()).thenReturn(mock(JobLevelListener.class));

    fakeApplicationListener = new ApplicationLevelListener(
        mockedSparkContext,
            fakeConfig,
            fakeTaskListener,
            fakeStageListener,
            fakeJobListener,
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
    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener.getWorkload() != null);
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener.getWorkload();
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);
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
    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener.getWorkload() != null);
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener.getWorkload();
    assertThat(workload.getDomain()).isEqualTo(fakeConfig.getDomain());
    assertThat(workload.getAuthors()).isEqualTo(fakeConfig.getAuthors());
    assertThat(workload.getWorkloadDescription()).isEqualTo(fakeConfig.getDescription());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getDateStart()).isEqualTo(sutStartTime);
    assertThat(workload.getDateEnd()).isEqualTo(applicationDateEnd);
  }

  @Test
  void workloadCountMetricsCollected() {
    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener.getWorkload() != null);
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener.getWorkload();
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
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeTaskListener.onTaskEnd(taskEndEvent1);
    fakeTaskListener.onTaskEnd(taskEndEvent2);
    fakeStageListener.onStageCompleted(stageCompleted1);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent4);
    fakeStageListener.onStageCompleted(stageCompleted2);

    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener.getWorkload() != null);

    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener.getWorkload();
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
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeStageListener.onStageCompleted(stageCompleted2);

    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener.getWorkload() != null);
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener.getWorkload();
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

    fakeTaskListener.onJobStart(jobStart1);
    fakeStageListener.onJobStart(jobStart1);
    fakeTaskListener.onTaskEnd(taskEndEvent1);
    fakeTaskListener.onTaskEnd(taskEndEvent2);
    fakeStageListener.onStageCompleted(stageCompleted1);
    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS)
        .until(() -> fakeApplicationListener.getProcessedObjects().count() == 0);

    fakeTaskListener.onJobStart(jobStart2);
    fakeStageListener.onJobStart(jobStart2);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent4);
    fakeStageListener.onStageCompleted(stageCompleted2);
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);
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
            fakeConfig,
            fakeTaskListener,
            fakeStageListener,
            fakeJobListener,
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

  @Test
  void computeMinWithZero() {
    ApplicationLevelListener listener = mock(ApplicationLevelListener.class);
    when(listener.computeMin(any(Stream.class))).thenCallRealMethod();
    List<Double> minList = List.of(-1.0, 0.0, 0.1, 0.03, 1.0, 891.0);
    Stream<Double> minStream = new Stream<>(minList);
    assertThat(listener.computeMin(minStream)).isEqualTo(0.0);
  }

  @Test
  void computeMinWithOnlyPositive() {
    ApplicationLevelListener listener = mock(ApplicationLevelListener.class);
    when(listener.computeMin(any(Stream.class))).thenCallRealMethod();
    List<Double> minList = List.of(1.0, 19.2, 0.1, 0.03, 1.0, 891.0);
    Stream<Double> minStream = new Stream<>(minList);
    assertThat(listener.computeMin(minStream)).isEqualTo(0.03);
  }

  @Test
  void computeMeanWithZero() {
    ApplicationLevelListener listener = mock(ApplicationLevelListener.class);
    when(listener.computeMean(any(Stream.class), anyLong())).thenCallRealMethod();
    List<Double> meanList = List.of(-1.0, 0.0, 1.0, 2.0);
    Stream<Double> meanStream = new Stream<>(meanList);
    assertThat(listener.computeMean(meanStream, 3)).isEqualTo(1.0);
  }

  @Test
  void computeMeanWithAllNegatives() {
    ApplicationLevelListener listener = mock(ApplicationLevelListener.class);
    when(listener.computeMean(any(Stream.class), anyLong())).thenCallRealMethod();
    List<Double> meanList = List.of(-1.0, -2.0, -5.0, -0.01);
    Stream<Double> meanStream = new Stream<>(meanList);
    assertThat(listener.computeMean(meanStream, 4)).isEqualTo(-1.0);
  }

  @Test
  void computeMeanWithOnlyPositive() {
    ApplicationLevelListener listener = mock(ApplicationLevelListener.class);
    when(listener.computeMean(any(Stream.class), anyLong())).thenCallRealMethod();
    List<Double> meanList = List.of(1.0, 2.0, 3.0);
    Stream<Double> meanStream = new Stream<>(meanList);
    assertThat(listener.computeMean(meanStream, 3)).isEqualTo(2.0);
  }

  @Test
  void aggregateMetricsFalseSetsDefaultAggregationValues() {
    fakeConfig = RuntimeConfig.builder()
            .authors(new String[] {"Harry Potter"})
            .domain(Domain.SCIENTIFIC)
            .isStageLevel(false)
            .description("Yer a wizard harry")
            .aggregateMetrics(false)
            .build();
    fakeTaskListener = new TaskLevelListener(mockedSparkContext, fakeConfig);
    fakeStageListener = new StageLevelListener(mockedSparkContext, fakeConfig);
    fakeJobListener = new JobLevelListener(mockedSparkContext, fakeConfig, fakeTaskListener, fakeStageListener);

    fakeApplicationListener = new ApplicationLevelListener(
            mockedSparkContext,
            fakeConfig,
            fakeTaskListener,
            fakeStageListener,
            fakeJobListener,
            sparkDataSource,
            mock(MetricStreamingEngine.class),
            mock(WtaWriter.class));

    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(testStageInfo1);
    stageBuffer.$plus$eq(testStageInfo2);
    SparkListenerJobStart jobStart = new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties());
    fakeTaskListener.onJobStart(jobStart);
    fakeStageListener.onJobStart(jobStart);
    fakeTaskListener.onTaskEnd(taskEndEvent1);
    fakeTaskListener.onTaskEnd(taskEndEvent2);
    fakeStageListener.onStageCompleted(stageCompleted1);
    fakeTaskListener.onTaskEnd(taskEndEvent3);
    fakeTaskListener.onTaskEnd(taskEndEvent4);
    fakeStageListener.onStageCompleted(stageCompleted2);

    assertThat(fakeApplicationListener.getProcessedObjects().isEmpty()).isTrue();
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    await().atMost(20, SECONDS).until(() -> fakeApplicationListener.getWorkload() != null);

    assertThat(fakeApplicationListener.getProcessedObjects().count()).isEqualTo(0);

    Workload workload = fakeApplicationListener.getWorkload();
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1.0);
  }
}
