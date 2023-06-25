package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DagSolverTest extends BaseLevelListenerTest {

  TaskLevelListener mockedListener1;

  TaskLevelListener mockedListener2;

  List<Task> stages1;

  List<Task> stages2;

  @BeforeEach
  void init() {
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
