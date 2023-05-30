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
    sut.registerTaskListener();
    sut.registerJobListener();
    invokeJob();
    invokeJob();
    assertThat(((JobLevelListener) sut.getJobLevelListener()).getJobSubmitTimes())
        .isEmpty();

    Workflow workflow = sut.getJobLevelListener().getProcessedObjects().get(1);
    assertThat(workflow.getId()).isGreaterThan(0L);
    assertThat(workflow.getSubmitTime()).isGreaterThan(0L);
    assertThat(workflow.getApplicationName())
        .isEqualTo(spark.sparkContext().getConf().get("spark.app.name"));
    assertThat(workflow.getScheduler()).isEqualTo("DAGScheduler");

    assertThat(workflow.getTasks()).isNotEmpty().isSortedAccordingTo(Comparator.comparing(Task::getSubmitTime));
    assertThat(sut.getJobLevelListener().getProcessedObjects())
        .hasSize(2)
        .isSortedAccordingTo(Comparator.comparing(Workflow::getSubmitTime));
  }

  @Test
  void jobsHaveNoTasksIfTaskListenerNotInvoked() {
    sut.registerJobListener();
    invokeJob();
    invokeJob();
    assertThat(sut.getJobLevelListener().getProcessedObjects())
        .hasSize(2)
        .allMatch(wf -> wf.getTasks().length == 0);
  }
}
