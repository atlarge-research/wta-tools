package com.asml.apa.wta.spark.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().count()).isGreaterThanOrEqualTo(0);
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
    sut1.removeListeners();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
  }
}
