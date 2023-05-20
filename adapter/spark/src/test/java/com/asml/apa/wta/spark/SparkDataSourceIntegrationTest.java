package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut.getTaskMetrics()).isEmpty();
  }

  @Test
  public void registeredTaskListenerCollectsMetrics() {
    sut.registerTaskListener();
    assertThat(sut.getTaskMetrics()).isEmpty();
    invokeJob();
    assertThat(sut.getTaskMetrics()).isNotEmpty();
  }

  @Test
  public void registeredTaskListenerKeepsCollectingMetrics() {
    sut.registerTaskListener();
    assertThat(sut.getTaskMetrics()).isEmpty();
    invokeJob();
    int size1 = sut.getTaskMetrics().size();
    assertThat(sut.getTaskMetrics()).isNotEmpty();
    invokeJob();
    int size2 = sut.getTaskMetrics().size();
    assertThat(size2).isGreaterThan(size1);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut.getTaskMetrics()).isEmpty();
    invokeJob();
    assertThat(sut.getTaskMetrics()).isEmpty();
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut.registerTaskListener();
    sut.removeTaskListener();
    invokeJob();
    assertThat(sut.getTaskMetrics()).isEmpty();
  }
}
