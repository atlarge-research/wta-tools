package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

class JobLevelListenerTest {

  SparkContext mockSparkContext;

  JobLevelListener sut;

  @BeforeEach
  void setup() {
    mockSparkContext = mock(SparkContext.class);
    when(mockSparkContext.getConf()).thenReturn(new SparkConf().set("spark.app.name", "testApp"));
    when(mockSparkContext.appName()).thenReturn("testApp");
    sut = new JobLevelListener(
        mockSparkContext,
        RuntimeConfig.builder().domain(Domain.ENGINEERING).build(),
        new TaskLevelListener(mockSparkContext, RuntimeConfig.builder().build()));
  }

  @Test
  void recordsTheTimeWhenJobIsSubmittedInMap() {
    sut.onJobStart(new SparkListenerJobStart(559, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    assertThat(sut.getJobSubmitTimes()).containsEntry(559, 40L);
  }

  @Test
  void jobStartAndEndStateIsCorrect() {
    sut.onJobStart(new SparkListenerJobStart(559, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    sut.onJobEnd(new SparkListenerJobEnd(559, 60L, new JobFailed(new RuntimeException("test"))));
    assertThat(sut.getJobSubmitTimes()).isEmpty();
    assertThat(sut.getProcessedObjects()).hasSize(1);

    Workflow sutWorkflow = sut.getProcessedObjects().get(0);
    assertThat(sutWorkflow.getId()).isEqualTo(559);
    assertThat(sutWorkflow.getSubmitTime()).isEqualTo(40L);
    assertThat(sutWorkflow.getScheduler()).isEqualTo("DAGScheduler");
    assertThat(sutWorkflow.getDomain()).isEqualTo(Domain.ENGINEERING);
    assertThat(sutWorkflow.getApplicationName()).isEqualTo("testApp");

    // assert that every field of sutWorkflow that is not mentioned above is not null
    assertThat(sutWorkflow.getNfrs()).isEmpty();
    assertThat(sutWorkflow.getApplicationField()).isEqualTo("ETL");
  }
}
