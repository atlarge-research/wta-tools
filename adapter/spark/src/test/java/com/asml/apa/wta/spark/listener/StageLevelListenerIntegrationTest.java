package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class StageLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testGetStageMetricsHasTasksAfterSparkJobAndYieldsNoErrors() throws InterruptedException {
    sut1.registerStageListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);
    assertThat(sut1.getStageLevelListener().getProcessedObjects().isEmpty()).isFalse();
  }

  @Test
  void runningAJobShouldInitializeWorkflow() throws InterruptedException {
    sut1.registerStageListener();
    invokeJob();
    stopJob();
    assertThat(sut1.getStageLevelListener().getStageToJob()).isNotEmpty();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut1.getStageLevelListener().getProcessedObjects().isEmpty()).isFalse();
    assertThat(sut1.getStageLevelListener().getProcessedObjects().head().getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
