package com.asml.apa.wta.spark.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
import org.junit.jupiter.api.BeforeEach;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

class BaseLevelListenerTest {

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

  @BeforeEach
  void setupCommonListenerDependencies() {
    // setup mock spark context

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
    mapResource2 = new HashMap<String, TaskResourceRequest>();
    SparkConf conf2 = new SparkConf().set("spark.app.name", "testApp");
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

    fakeApplicationListener1 = new ApplicationLevelListener(
        mockedSparkContext, fakeConfig1, fakeTaskListener1, fakeStageListener1, fakeJobListener1);

    fakeConfig2 = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .isStageLevel(true)
        .description("Yer a wizard harry")
        .build();
    fakeStageListener2 = new StageLevelListener(mockedSparkContext2, fakeConfig2);

    fakeTaskListener2 = new TaskLevelListener(mockedSparkContext2, fakeConfig2);

    fakeStageListener2 = new StageLevelListener(mockedSparkContext2, fakeConfig2);

    fakeJobListener2 =
        new JobLevelListener(mockedSparkContext2, fakeConfig2, fakeTaskListener2, fakeStageListener2);

    fakeApplicationListener2 = new ApplicationLevelListener(
        mockedSparkContext2, fakeConfig2, fakeTaskListener2, fakeStageListener2, fakeJobListener2);
  }
}
