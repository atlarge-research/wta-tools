package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class TaskLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testGetTaskMetricsHasTasksAfterSparkJobAndYieldsNoErrors() {
    sut1.registerTaskListener();
    invokeJob();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).isNotEmpty();
  }

  @Test
  void runningAJobShouldInitializeWorkflow() {
    sut1.registerTaskListener();
    invokeJob();
    invokeJob();
    assertThat(sut1.getTaskLevelListener().getStageToJob()).isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(0).getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
