package com.asml.apa.wta.spark.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isEmpty();
  }

  @Test
  public void registeredTaskListenerCollectsMetrics() {
    sut1.registerTaskListener();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isEmpty();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isNotEmpty();
  }

  @Test
  public void registeredTaskListenerKeepsCollectingMetrics() {
    sut1.registerTaskListener();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isEmpty();
    invokeJob();
    int size1 = sut1.getTaskLevelListener().getProcessedObjects().size();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isNotEmpty();
    invokeJob();
    int size2 = sut1.getTaskLevelListener().getProcessedObjects().size();
    assertThat(size2).isGreaterThan(size1);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isEmpty();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isEmpty();
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut1.registerTaskListener();
    sut1.removeTaskListener();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isEmpty();
  }
}
