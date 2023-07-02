package com.asml.apa.wta.spark.listener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.collection.mutable.ListBuffer;

class StageLevelListenerTest {

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

  private StageLevelListener fakeStageListener1;

  private StageLevelListener fakeStageListener2;

  private SparkListenerStageCompleted stageEndEvent;

  private StageInfo testStageInfo;

  private StageInfo spyStageInfo;

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

    fakeStageListener2 = new StageLevelListener(mockedSparkContext2, fakeConfig2);

    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);
    ShuffleWriteMetrics mockedShuffleMetrics = mock(ShuffleWriteMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);
    when(mockedMetrics.peakExecutionMemory()).thenReturn(100L);
    when(mockedMetrics.diskBytesSpilled()).thenReturn(100L);
    when(mockedMetrics.shuffleWriteMetrics()).thenReturn(mockedShuffleMetrics);
    when(mockedShuffleMetrics.bytesWritten()).thenReturn(100L);
    ListBuffer<Integer> parents = new ListBuffer<>();
    parents.$plus$eq(1);
    parents.$plus$eq(2);

    testStageInfo = new StageInfo(
        0,
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

    spyStageInfo = spy(testStageInfo);
    Option<Object> submissionTimeOption = Option.apply(10L);
    when(spyStageInfo.submissionTime()).thenReturn(submissionTimeOption);
    stageEndEvent = new SparkListenerStageCompleted(spyStageInfo);
  }

  @Test
  void testStageEndMetricExtraction() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(spyStageInfo);

    fakeStageListener2.onJobStart(new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties()));
    fakeStageListener2.onStageCompleted(stageEndEvent);

    await().atMost(20, SECONDS)
        .until(() -> fakeStageListener2.getProcessedObjects().count() == 1);

    Task curStage = fakeStageListener2.getProcessedObjects().head();
    assertEquals(1, curStage.getId());
    assertEquals("", curStage.getType());
    assertEquals(10L, curStage.getTsSubmit());
    assertEquals(100L, curStage.getRuntime());
    assertEquals(1L, curStage.getWorkflowId());
    assertEquals(Math.abs("testUser".hashCode()), curStage.getUserId());
    assertEquals(-1, curStage.getSubmissionSite());
    assertEquals("N/A", curStage.getResourceType());
    assertEquals(-1.0, curStage.getResourceAmountRequested());
    assertEquals(-1.0, curStage.getMemoryRequested());
    assertEquals(200.0, curStage.getDiskSpaceRequested());
    assertEquals(-1L, curStage.getEnergyConsumption());
    assertEquals(-1L, curStage.getNetworkIoTime());
    assertEquals(-1L, curStage.getDiskIoTime());
    assertEquals(-1, curStage.getGroupId());
    assertEquals("", curStage.getNfrs());
    assertEquals("", curStage.getParams());
    assertEquals(2, curStage.getParents().length);
    assertEquals(0, curStage.getChildren().length);
  }

  @Test
  void onStageCompletedTest() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(spyStageInfo);

    fakeStageListener1.onJobStart(new SparkListenerJobStart(0, 2L, stageBuffer.toList(), new Properties()));
    fakeStageListener1.onStageCompleted(stageEndEvent);
    assertThat(fakeStageListener1.getStageToParents()).containsEntry(1L, new Long[] {2L, 3L});
    assertThat(fakeStageListener1.getStageToParents().size()).isEqualTo(1);
    List<Long> childrenStages = new ArrayList<>();
    childrenStages.add(1L);
    assertThat(fakeStageListener1.getParentStageToChildrenStages()).containsEntry(2L, childrenStages);
    assertThat(fakeStageListener1.getParentStageToChildrenStages()).containsEntry(3L, childrenStages);
    assertThat(fakeStageListener1.getParentStageToChildrenStages().size()).isEqualTo(2);
  }
}
