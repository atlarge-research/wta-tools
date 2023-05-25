package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Workload;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationLevelListenerTest extends BaseLevelListenerTest {

  SparkListenerApplicationEnd applicationEndObj;

  @BeforeEach
  void setup() {
    applicationEndObj = new SparkListenerApplicationEnd(mockedSparkContext.startTime() + 1000L);
  }

  @Test
  void applicationCollectsDesiredInformationEvenIfINcomplete() {
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(workload.getWorkflows()).hasSize(0);
    assertThat(workload.getWorkflows().length).isEqualTo(workload.getTotal_workflows());
    assertThat(workload.getTotal_tasks()).isEqualTo(0);
    assertThat(workload.getDomain()).isEqualTo(fakeConfig.getDomain());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getDate_start()).isEqualTo(sutStartTime);
    assertThat(workload.getDate_end()).isEqualTo(sutStartTime + 1000L);
  }
}
