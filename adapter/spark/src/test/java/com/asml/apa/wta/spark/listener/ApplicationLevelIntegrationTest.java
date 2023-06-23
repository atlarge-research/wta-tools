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
    invokeJob();
    invokeJob();
    stopJob();

    assertThat(sut1.getTaskLevelListener().getProcessedObjects().size()).isEqualTo(6);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(0).getParents())
        .isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(0).getChildren())
        .isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(1).getParents())
        .isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(1).getChildren())
        .isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(2).getParents())
        .isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(2).getChildren())
        .isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(3).getParents())
        .isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(3).getChildren())
        .isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(4).getParents())
        .isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(4).getChildren())
        .isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(5).getParents())
        .isNotEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(5).getChildren())
        .isEmpty();
  }

  @Test
  void testSparkStageParentChildrenField() {
    sut2.registerStageListener();
    sut2.registerJobListener();
    sut2.registerApplicationListener();
    invokeJob();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().size()).isEqualTo(6);
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(0).getParents())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(0).getChildren())
        .isNotEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(1).getParents())
        .isNotEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(1).getChildren())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(2).getParents())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(2).getChildren())
        .isNotEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(3).getParents())
        .isNotEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(3).getChildren())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(4).getParents())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(4).getChildren())
        .isNotEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(5).getParents())
        .isNotEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(5).getChildren())
        .isEmpty();
  }
}
