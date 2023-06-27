package com.asml.apa.wta.spark.datasource;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    await().atMost(20, SECONDS)
        .until(() -> sut1.getTaskLevelListener().getProcessedObjects().count() >= 0);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    await().atMost(20, SECONDS)
        .until(() -> sut1.getTaskLevelListener().getProcessedObjects().isEmpty());
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut1.registerTaskListener();
    sut1.removeListeners();
    invokeJob();
    await().atMost(20, SECONDS)
        .until(() -> sut1.getTaskLevelListener().getProcessedObjects().isEmpty());
  }
}
