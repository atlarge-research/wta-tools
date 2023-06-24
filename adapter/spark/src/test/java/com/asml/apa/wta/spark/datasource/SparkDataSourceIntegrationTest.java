package com.asml.apa.wta.spark.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import com.asml.apa.wta.spark.listener.AbstractListener;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SparkDataSourceIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().count()).isGreaterThanOrEqualTo(0);
  }

  @Test
  public void registeredTaskListenerCollectsMetrics() throws InterruptedException {
    sut1.registerTaskListener();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    AbstractListener.getThreadPool().awaitTermination(5, TimeUnit.SECONDS);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isFalse();
  }

  @Test
  public void registeredTaskListenerKeepsCollectingMetrics() throws InterruptedException {
    sut1.registerTaskListener();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    AbstractListener.getThreadPool().awaitTermination(5, TimeUnit.SECONDS);
    long size1 = sut1.getTaskLevelListener().getProcessedObjects().count();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isFalse();
    AbstractListener.getThreadPool().awaitTermination(5, TimeUnit.SECONDS);
    invokeJob();
    long size2 = sut1.getTaskLevelListener().getProcessedObjects().count();
    assertThat(size2).isGreaterThan(size1);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() throws InterruptedException {
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    AbstractListener.getThreadPool().awaitTermination(5, TimeUnit.SECONDS);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
  }

  @Test
  public void removedTaskListenerDoesNotCollect() throws InterruptedException {
    sut1.registerTaskListener();
    sut1.removeListeners();
    invokeJob();
    AbstractListener.getThreadPool().awaitTermination(5, TimeUnit.SECONDS);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
  }
}
