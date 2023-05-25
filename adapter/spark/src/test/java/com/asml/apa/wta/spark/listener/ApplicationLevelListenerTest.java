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
  void applicationCollectsDesiredInformationEvenIfINotcomplete() {
    fakeApplicationListener.onApplicationEnd(applicationEndObj);
    assertThat(fakeApplicationListener.getProcessedObjects()).hasSize(1);

    Workload workload = fakeApplicationListener.getProcessedObjects().get(0);
    assertThat(workload.getWorkflows()).hasSize(0);
    assertThat(workload.getWorkflows().length).isEqualTo(workload.getTotalWorkflows());
    assertThat(workload.getTotalTasks()).isEqualTo(0);
    assertThat(workload.getDomain()).isEqualTo(fakeConfig.getDomain());
    long sutStartTime = mockedSparkContext.startTime();
    assertThat(workload.getStartDate()).isEqualTo(sutStartTime);
    assertThat(workload.getEndDate()).isEqualTo(sutStartTime + 1000L);
  }
}
