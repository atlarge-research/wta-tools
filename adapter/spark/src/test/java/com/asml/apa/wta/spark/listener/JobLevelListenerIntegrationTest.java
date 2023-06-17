package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

class JobLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void testGetJobMetricsHasJobsAfterSparkJobAndYieldsNoErrors() {
    sut1.registerTaskListener();
    sut1.registerJobListener();
    invokeJob();
    invokeJob();
    assertThat(((JobLevelListener) sut1.getJobLevelListener()).getJobSubmitTimes())
        .isEmpty();

    Workflow workflow = sut1.getJobLevelListener().getProcessedObjects().get(1);
    assertThat(workflow.getId()).isGreaterThan(0L);
    assertThat(workflow.getTsSubmit()).isGreaterThan(0L);
    assertThat(workflow.getApplicationName())
        .isEqualTo(spark.sparkContext().getConf().get("spark.app.name"));
    assertThat(workflow.getScheduler()).isEqualTo("FIFO");

    assertThat(workflow.getTasks()).isNotEmpty().isSortedAccordingTo(Comparator.comparing(Task::getTsSubmit));
    assertThat(sut1.getJobLevelListener().getProcessedObjects())
        .hasSize(2)
        .isSortedAccordingTo(Comparator.comparing(Workflow::getTsSubmit));
  }

  @Test
  void jobsHaveNoTasksIfTaskListenerNotInvoked() {
    sut1.registerJobListener();
    invokeJob();
    invokeJob();
    assertThat(sut1.getJobLevelListener().getProcessedObjects())
        .hasSize(2)
        .allMatch(wf -> wf.getTasks().length == 0);
  }
}
