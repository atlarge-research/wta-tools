package com.asml.apa.wta.spark.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isEmpty();
  }

  @Test
  public void registeredTaskListenerCollectsMetrics() {
    sut.registerTaskListener();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isEmpty();
    invokeJob();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isNotEmpty();
  }

  @Test
  public void registeredTaskListenerKeepsCollectingMetrics() {
    sut.registerTaskListener();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isEmpty();
    invokeJob();
    int size1 = sut.getTaskLevelListener().getProcessedObjects().size();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isNotEmpty();
    invokeJob();
    int size2 = sut.getTaskLevelListener().getProcessedObjects().size();
    assertThat(size2).isGreaterThan(size1);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isEmpty();
    invokeJob();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isEmpty();
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut.registerTaskListener();
    sut.removeTaskListener();
    invokeJob();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isEmpty();
  }
}
