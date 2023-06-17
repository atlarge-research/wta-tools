package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

public class StageLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {
  @Test
  void testGetStageMetricsHasTasksAfterSparkJobAndYieldsNoErrors() {
    sut1.registerStageListener();
    invokeJob();
    assertThat(sut1.getStageLevelListener().getProcessedObjects()).isNotEmpty();
  }

  @Test
  void runningAJobShouldClearTheMapOfEntriesAfterStageIsDoneWorkflowIdShouldBeInitialised() {
    sut1.registerStageListener();
    invokeJob();
    assertThat(((StageLevelListener) sut1.getStageLevelListener()).getStageToJob())
        .isEmpty();
    assertThat(sut1.getStageLevelListener().getProcessedObjects()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut1.getStageLevelListener().getProcessedObjects().get(0).getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
