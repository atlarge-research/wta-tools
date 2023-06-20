package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

public class StageLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testGetStageMetricsHasTasksAfterSparkJobAndYieldsNoErrors() {
    sut1.registerStageListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut1.getStageLevelListener().getProcessedObjects().isEmpty()).isFalse();
  }

  @Test
  void runningAJobShouldInitializeWorkflow() {
    sut1.registerStageListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut1.getStageLevelListener().getStageToJob()).isNotEmpty();
    assertThat(sut1.getStageLevelListener().getProcessedObjects().isEmpty()).isFalse();
    assertThat(sut1.getStageLevelListener().getProcessedObjects().head().getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
