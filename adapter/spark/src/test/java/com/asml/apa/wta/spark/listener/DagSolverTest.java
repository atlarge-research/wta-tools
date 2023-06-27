package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.resource.TaskResourceRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class DagSolverTest {

  protected SparkContext mockedSparkContext;

  protected ResourceProfileManager mockedResourceProfileManager;

  protected ResourceProfile mockedResource;

  protected scala.collection.immutable.Map<String, TaskResourceRequest> mapResource;

  protected SparkContext mockedSparkContext2;

  protected ResourceProfileManager mockedResourceProfileManager2;

  protected ResourceProfile mockedResource2;

  protected scala.collection.immutable.Map<String, TaskResourceRequest> mapResource2;

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

  TaskLevelListener mockedListener1;

  TaskLevelListener mockedListener2;

  List<Task> stages1;

  List<Task> stages2;

  @BeforeEach
  void init() {
    mockedSparkContext = mock(SparkContext.class);
    mockedResourceProfileManager = mock(ResourceProfileManager.class);
    mockedResource = mock(ResourceProfile.class);
    mapResource = new scala.collection.immutable.HashMap<String, TaskResourceRequest>()
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
    mapResource2 = new scala.collection.immutable.HashMap<>();
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
    Task mockedTask1 = mock(Task.class);
    when(mockedTask1.getRuntime()).thenReturn(1L);
    Task mockedTask2 = mock(Task.class);
    when(mockedTask2.getRuntime()).thenReturn(2L);
    Task mockedTask3 = mock(Task.class);
    when(mockedTask3.getRuntime()).thenReturn(4L);
    Map<Long, List<Task>> stt1 = new HashMap<>();
    stt1.put(0L, List.of(mockedTask1));
    stt1.put(1L, List.of(mockedTask1));
    stt1.put(2L, List.of(mockedTask2));
    stt1.put(3L, List.of(mockedTask1));
    mockedListener1 = mock(TaskLevelListener.class);
    when(mockedListener1.getStageToTasks()).thenReturn(stt1);

    Map<Long, List<Task>> stt2 = new HashMap<>();
    stt2.put(0L, List.of(mockedTask1));
    stt2.put(1L, List.of(mockedTask2));
    stt2.put(2L, List.of(mockedTask2));
    stt2.put(3L, List.of(mockedTask1));
    stt2.put(4L, List.of(mockedTask2));
    stt2.put(5L, List.of(mockedTask1));
    stt2.put(6L, List.of(mockedTask3));
    mockedListener2 = mock(TaskLevelListener.class);
    when(mockedListener2.getStageToTasks()).thenReturn(stt2);

    stages1 = new ArrayList<>();
    stages1.add(
        Task.builder().id(1).parents(new long[0]).type("").runtime(1).build());
    stages1.add(
        Task.builder().id(2).parents(new long[] {1}).type("").runtime(1).build());
    stages1.add(
        Task.builder().id(3).parents(new long[] {1}).type("").runtime(2).build());
    stages1.add(Task.builder()
        .id(4)
        .parents(new long[] {2, 3})
        .type("")
        .runtime(1)
        .build());

    stages2 = new ArrayList<>();
    stages2.add(
        Task.builder().id(1).parents(new long[0]).type("").runtime(1).build());
    stages2.add(
        Task.builder().id(2).parents(new long[0]).type("").runtime(2).build());
    stages2.add(
        Task.builder().id(3).parents(new long[] {1}).type("").runtime(2).build());
    stages2.add(Task.builder()
        .id(4)
        .parents(new long[] {1, 2})
        .type("")
        .runtime(1)
        .build());
    stages2.add(Task.builder()
        .id(5)
        .parents(new long[] {1, 2})
        .type("")
        .runtime(2)
        .build());
    stages2.add(
        Task.builder().id(6).parents(new long[] {3}).type("").runtime(1).build());
    stages2.add(Task.builder()
        .id(7)
        .parents(new long[] {4, 5})
        .type("")
        .runtime(4)
        .build());
  }

  @Test
  void test() {
    List<Long> cp = fakeJobListener1.solveCriticalPath(stages1).stream()
        .map(Task::getId)
        .collect(Collectors.toList());
    assertThat(cp.size()).isEqualTo(3);
    assertThat(cp.contains(1L)).isTrue();
    assertThat(cp.contains(3L)).isTrue();
    assertThat(cp.contains(4L)).isTrue();
  }

  @Test
  void augTest() {
    List<Long> cp = fakeJobListener1.solveCriticalPath(stages2).stream()
        .map(Task::getId)
        .collect(Collectors.toList());
    assertThat(cp.size()).isEqualTo(3);
    assertThat(cp.contains(2L)).isTrue();
    assertThat(cp.contains(5L)).isTrue();
    assertThat(cp.contains(7L)).isTrue();
  }
}
