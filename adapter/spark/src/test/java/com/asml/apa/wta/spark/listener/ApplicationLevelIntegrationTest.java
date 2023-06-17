package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class ApplicationLevelIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testSparkTaskParentChildrenField() {
    sut1.registerTaskListener();
    sut1.registerStageListener();
    sut1.registerJobListener();
    sut1.registerApplicationListener();
    invokeJob();
    stopJob();

    assertThat(sut1.getTaskLevelListener().getProcessedObjects().size()).isEqualTo(2);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(0).getParents())
        .isEqualTo(new long[0]);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(0).getChildren())
        .isEqualTo(new long[] {2});
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(1).getParents())
        .isEqualTo(new long[] {1});
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(1).getChildren())
        .isEqualTo(new long[0]);
  }

  @Test
  void testSparkStageParentChildrenField() {
    sut2.registerTaskListener();
    sut2.registerStageListener();
    sut2.registerJobListener();
    sut2.registerApplicationListener();
    invokeJob();
    stopJob();

    assertThat(sut2.getStageLevelListener().getProcessedObjects().size()).isEqualTo(2);
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(0).getParents())
        .isEqualTo(new long[0]);
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(0).getChildren())
        .isEqualTo(new long[] {2});
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(1).getParents())
        .isEqualTo(new long[] {1});
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(1).getChildren())
        .isEqualTo(new long[0]);
  }
}
