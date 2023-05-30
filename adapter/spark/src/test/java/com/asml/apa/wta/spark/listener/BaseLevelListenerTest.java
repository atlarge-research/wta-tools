package com.asml.apa.wta.spark.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeEach;

public class BaseLevelListenerTest {

  protected SparkContext mockedSparkContext;

  protected RuntimeConfig fakeConfig;

  protected AbstractListener<Task> fakeTaskListener;
  protected StageLevelListener fakeStageListener;
  protected AbstractListener<Workflow> fakeJobListener;
  protected AbstractListener<Workload> fakeApplicationListener;

  @BeforeEach
  void setupCommonListenerDependencies() {
    // setup mock spark context
    mockedSparkContext = mock(SparkContext.class);
    SparkConf conf = new SparkConf().set("spark.app.name", "testApp");
    when(mockedSparkContext.sparkUser()).thenReturn("testUser");
    when(mockedSparkContext.getConf()).thenReturn(conf);
    when(mockedSparkContext.appName()).thenReturn("testApp");
    when(mockedSparkContext.startTime()).thenReturn(5000L);

    // setup fake config
    fakeConfig = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .description("Yer a wizard harry")
        .events(Map.of("event1", "Desc of event1", "event2", "Desc of event2"))
        .build();
    fakeStageListener = new StageLevelListener(mockedSparkContext, fakeConfig);

    fakeTaskListener = new TaskLevelListener(mockedSparkContext, fakeConfig, fakeStageListener);

    fakeJobListener = new JobLevelListener(mockedSparkContext, fakeConfig, fakeTaskListener);

    fakeApplicationListener = new ApplicationLevelListener(mockedSparkContext, fakeConfig, fakeJobListener, fakeTaskListener);
  }
}
