package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class TaskLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testGetTaskMetricsHasTasksAfterSparkJobAndYieldsNoErrors() {
    sut.registerTaskListener();
    invokeJob();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).isNotEmpty();
  }

  @Test
  void runningAJobShouldClearTheMapOfEntriesAfterStageIsDoneWorkflowIdShouldBeInitialised() {
    sut.registerTaskListener();
    invokeJob();
    assertThat(((TaskLevelListener) sut.getTaskLevelListener()).getStageToJob())
        .isEmpty();
    assertThat(sut.getTaskLevelListener().getProcessedObjects()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(0).getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
