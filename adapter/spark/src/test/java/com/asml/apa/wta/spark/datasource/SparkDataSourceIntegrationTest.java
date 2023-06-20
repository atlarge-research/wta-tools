package com.asml.apa.wta.spark.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
  }

  @Test
  public void registeredTaskListenerCollectsMetrics() {
    sut1.registerTaskListener();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isFalse();
  }

  @Test
  public void registeredTaskListenerKeepsCollectingMetrics() {
    sut1.registerTaskListener();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    long size1 = sut1.getTaskLevelListener().getProcessedObjects().count();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isFalse();
    invokeJob();
    long size2 = sut1.getTaskLevelListener().getProcessedObjects().count();
    assertThat(size2).isGreaterThan(size1);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut1.registerTaskListener();
    sut1.removeTaskListener();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
  }
}
