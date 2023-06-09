package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import org.junit.jupiter.api.Test;

class ApplicationLevelIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testParentChildrenField() {
    sut.registerTaskListener();
    sut.registerStageListener();
    sut.registerJobListener();
    sut.registerApplicationListener();
    invokeJob();
    invokeJob();
    invokeJob();
    stopJob();

    assertThat(sut.getTaskLevelListener().getProcessedObjects().size()).isEqualTo(6);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(0).getParents())
        .isEqualTo(new long[0]);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(0).getChildren())
        .isEqualTo(new long[] {2});
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(1).getParents())
        .isEqualTo(new long[] {1});
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(1).getChildren())
        .isEqualTo(new long[0]);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(2).getParents())
        .isEqualTo(new long[0]);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(2).getChildren())
        .isEqualTo(new long[] {4});
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(3).getParents())
        .isEqualTo(new long[] {3});
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(3).getChildren())
        .isEqualTo(new long[0]);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(4).getParents())
        .isEqualTo(new long[0]);
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(4).getChildren())
        .isEqualTo(new long[] {6});
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(5).getParents())
        .isEqualTo(new long[] {5});
    assertThat(sut.getTaskLevelListener().getProcessedObjects().get(5).getChildren())
        .isEqualTo(new long[0]);
  }
}
