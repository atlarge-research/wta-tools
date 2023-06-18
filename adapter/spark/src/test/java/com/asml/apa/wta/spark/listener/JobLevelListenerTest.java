package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Workflow;
import java.util.Properties;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

class JobLevelListenerTest extends BaseLevelListenerTest {

  @Test
  void recordsTheTimeWhenJobIsSubmittedInMap() {
    fakeJobListener1.onJobStart(
        new SparkListenerJobStart(559, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    assertThat(fakeJobListener1.getJobSubmitTimes()).containsEntry(560L, 40L);
  }

  @Test
  void jobStartAndEndStateIsCorrect() {
    fakeJobListener1.onJobStart(
        new SparkListenerJobStart(559, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    fakeJobListener1.onJobEnd(new SparkListenerJobEnd(559, 60L, new JobFailed(new RuntimeException("test"))));
    assertThat(fakeJobListener1.getJobSubmitTimes()).isEmpty();
    assertThat(fakeJobListener1.getProcessedObjects()).hasSize(1);

    Workflow fakeJobListenerWorkflow =
        fakeJobListener1.getProcessedObjects().get(0);
    assertThat(fakeJobListenerWorkflow.getId()).isEqualTo(560);
    assertThat(fakeJobListenerWorkflow.getTsSubmit()).isEqualTo(40L);
    assertThat(fakeJobListenerWorkflow.getScheduler()).isEqualTo("FIFO");
    assertThat(fakeJobListenerWorkflow.getApplicationName()).isEqualTo("testApp");

    assertThat(fakeJobListenerWorkflow.getNfrs()).isEmpty();
    assertThat(fakeJobListenerWorkflow.getApplicationField()).isEqualTo("ETL");
  }
}
