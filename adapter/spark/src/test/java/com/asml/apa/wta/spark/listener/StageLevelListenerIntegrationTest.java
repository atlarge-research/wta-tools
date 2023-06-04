package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

public class StageLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {
  @Test
  void testGetStageMetricsHasTasksAfterSparkJobAndYieldsNoErrors() {
    sut.registerStageListener();
    invokeJob();
    assertThat(sut.getStageLevelListener().getProcessedObjects()).isNotEmpty();
  }

  @Test
  void runningAJobShouldClearTheMapOfEntriesAfterStageIsDoneWorkflowIdShouldBeInitialised() {
    sut.registerStageListener();
    invokeJob();
    assertThat(((StageLevelListener) sut.getStageLevelListener()).getStageIdsToJobs())
        .isEmpty();
    assertThat(sut.getStageLevelListener().getProcessedObjects()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut.getStageLevelListener().getProcessedObjects().get(0).getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
