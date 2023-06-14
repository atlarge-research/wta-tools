package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import java.util.List;
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
    stopJob();
    List<Task> processedObjects = sut.getTaskLevelListener().getProcessedObjects();
    assertThat(processedObjects.size()).isEqualTo(4);
    assertThat(processedObjects.get(0).getParents()).isEmpty();
    assertThat(processedObjects.get(0).getChildren()).isNotEmpty();
    assertThat(processedObjects.get(1).getParents()).isNotEmpty();
    assertThat(processedObjects.get(1).getChildren()).isEmpty();
    assertThat(processedObjects.get(2).getParents()).isEmpty();
    assertThat(processedObjects.get(2).getChildren()).isNotEmpty();
    assertThat(processedObjects.get(3).getParents()).isNotEmpty();
    assertThat(processedObjects.get(3).getChildren()).isEmpty();
  }
}
