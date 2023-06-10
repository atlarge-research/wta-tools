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
    fakeJobListener.onJobStart(
        new SparkListenerJobStart(559, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    assertThat(fakeJobListener.getJobSubmitTimes()).containsEntry(560, 40L);
  }

  @Test
  void jobStartAndEndStateIsCorrect() {
    fakeJobListener.onJobStart(
        new SparkListenerJobStart(559, 40L, new ListBuffer<StageInfo>().toList(), new Properties()));
    fakeJobListener.onJobEnd(new SparkListenerJobEnd(559, 60L, new JobFailed(new RuntimeException("test"))));
    assertThat(fakeJobListener.getJobSubmitTimes()).isEmpty();
    assertThat(fakeJobListener.getProcessedObjects()).hasSize(1);

    Workflow fakeJobListenerWorkflow = fakeJobListener.getProcessedObjects().get(0);
    assertThat(fakeJobListenerWorkflow.getId()).isEqualTo(560);
    assertThat(fakeJobListenerWorkflow.getTsSubmit()).isEqualTo(40L);
    assertThat(fakeJobListenerWorkflow.getScheduler()).isEqualTo("DAGScheduler");
    assertThat(fakeJobListenerWorkflow.getApplicationName()).isEqualTo("testApp");

    assertThat(fakeJobListenerWorkflow.getNfrs()).isEmpty();
    assertThat(fakeJobListenerWorkflow.getApplicationField()).isEqualTo("ETL");
  }
}
