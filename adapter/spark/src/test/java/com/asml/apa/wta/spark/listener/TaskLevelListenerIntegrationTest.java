package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TaskLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testGetTaskMetricsHasTasksAfterSparkJobAndYieldsNoErrors() throws InterruptedException {
    sut1.registerTaskListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isFalse();
  }

  @Test
  void runningAJobShouldInitializeWorkflow() throws InterruptedException {
    sut1.registerTaskListener();
    invokeJob();
    stopJob();
    assertThat(sut1.getTaskLevelListener().getStageToJob()).isNotEmpty();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut1.getTaskLevelListener().getProcessedObjects().isEmpty()).isFalse();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().head().getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
