package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DAGsolverTest extends BaseLevelListenerTest {

  List<Task> stages1;

  List<Task> stages2;

  JobLevelListener listener;

  @BeforeEach
  void init() {
    stages1 = new ArrayList<>();
    stages1.add(Task.builder().id(0).runtime(1).build());
    stages1.add(Task.builder().id(1).parents(new long[] {0}).runtime(1).build());
    stages1.add(Task.builder().id(2).parents(new long[] {0}).runtime(2).build());
    stages1.add(Task.builder().id(3).parents(new long[] {1, 2}).runtime(1).build());

    stages2 = new ArrayList<>();
    stages2.add(Task.builder().id(0).runtime(1).build());
    stages2.add(Task.builder().id(1).runtime(2).build());
    stages2.add(Task.builder().id(2).parents(new long[] {0}).runtime(2).build());
    stages2.add(Task.builder().id(3).parents(new long[] {0, 1}).runtime(1).build());
    stages2.add(Task.builder().id(4).parents(new long[] {0, 1}).runtime(2).build());
    stages2.add(Task.builder().id(5).parents(new long[] {2}).runtime(1).build());
    stages2.add(Task.builder().id(6).parents(new long[] {3, 4}).runtime(4).build());
  }

  @Test
  void test() {
    List<Long> cp = fakeJobListener.solveCriticalPath(stages1).stream()
        .map(Task::getId)
        .collect(Collectors.toList());
    assertThat(cp.size()).isEqualTo(3);
    System.out.println(cp);
    assertThat(cp.contains(0L)).isTrue();
    assertThat(cp.contains(2L)).isTrue();
    assertThat(cp.contains(3L)).isTrue();
  }

  @Test
  void augTest() {
    List<Long> cp = fakeJobListener.solveCriticalPath(stages2).stream()
        .map(Task::getId)
        .collect(Collectors.toList());
    assertThat(cp.size()).isEqualTo(3);
    System.out.println(cp);
    assertThat(cp.contains(1L)).isTrue();
    assertThat(cp.contains(4L)).isTrue();
    assertThat(cp.contains(6L)).isTrue();
  }
}
