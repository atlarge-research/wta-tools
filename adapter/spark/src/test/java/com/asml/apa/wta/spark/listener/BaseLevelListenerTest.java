package com.asml.apa.wta.spark.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.enums.Domain;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
import org.junit.jupiter.api.BeforeEach;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

public class BaseLevelListenerTest {

  protected SparkContext mockedSparkContext;

  protected ResourceProfileManager mockedResourceProfileManager;

  protected ResourceProfile mockedResource;

  protected Map<String, TaskResourceRequest> mapResource;

  protected RuntimeConfig fakeConfig;

  protected TaskLevelListener fakeTaskListener;
  protected StageLevelListener fakeStageListener;
  protected JobLevelListener fakeJobListener;
  protected ApplicationLevelListener fakeApplicationListener;

  @BeforeEach
  void setupCommonListenerDependencies() {
    // setup mock spark context

    mockedSparkContext = mock(SparkContext.class);
    mockedResourceProfileManager = mock(ResourceProfileManager.class);
    mockedResource = mock(ResourceProfile.class);
    mapResource = new HashMap<String, TaskResourceRequest>()
        .$plus(new Tuple2<>("this", new TaskResourceRequest("this", -1.0)));
    SparkConf conf = new SparkConf().set("spark.app.name", "testApp");
    when(mockedSparkContext.sparkUser()).thenReturn("testUser");
    when(mockedSparkContext.getConf()).thenReturn(conf);
    when(mockedSparkContext.appName()).thenReturn("testApp");
    when(mockedSparkContext.startTime()).thenReturn(5000L);
    when(mockedSparkContext.resourceProfileManager()).thenReturn(mockedResourceProfileManager);
    when(mockedResourceProfileManager.resourceProfileFromId(100)).thenReturn(mockedResource);
    when(mockedResource.taskResources()).thenReturn(mapResource);

    // setup fake config
    fakeConfig = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .description("Yer a wizard harry")
        .build();
    fakeStageListener = new StageLevelListener(mockedSparkContext, fakeConfig);

    fakeTaskListener = new TaskLevelListener(mockedSparkContext, fakeConfig);

    fakeStageListener = new StageLevelListener(mockedSparkContext, fakeConfig);

    fakeJobListener = new JobLevelListener(mockedSparkContext, fakeConfig, fakeTaskListener, fakeStageListener);

    fakeApplicationListener = new ApplicationLevelListener(
        mockedSparkContext, fakeConfig, fakeJobListener, fakeTaskListener, fakeStageListener);
  }
}
