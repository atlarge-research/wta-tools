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
    sut1.registerStageListener();
    sut1.registerJobListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut1.getJobLevelListener().getJobSubmitTimes()).isEmpty();
    assertThat(sut1.getJobLevelListener().getCriticalPathTasks()).isEmpty();

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
    stopJob();
    assertThat(sut1.getJobLevelListener().getProcessedObjects())
        .hasSizeGreaterThan(0)
        .allMatch(wf -> wf.getTasks().length == 0);
  }

  @Test
  void testSparkTaskProcessedObjects() {
    sut1.registerTaskListener();
    sut1.registerStageListener();
    sut1.registerJobListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).hasSizeGreaterThan(0);
    assertThat(sut1.getStageLevelListener().getProcessedObjects()).hasSizeGreaterThan(0);
    assertThat(sut1.getJobLevelListener().getProcessedObjects()).hasSizeGreaterThan(0);
  }

  @Test
  void testSparkStageProcessedObjects() {
    sut2.registerStageListener();
    sut2.registerJobListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut2.getTaskLevelListener().getProcessedObjects()).hasSize(0);
    assertThat(sut2.getStageLevelListener().getProcessedObjects()).hasSizeGreaterThan(0);
    assertThat(sut2.getJobLevelListener().getProcessedObjects()).hasSizeGreaterThan(0);
  }

  @Test
  void endOfJobShouldClearTheMapOfEntriesAfterJobIsDoneButNotProcessedObjectsTaskLevel() {
    sut1.registerTaskListener();
    sut1.registerStageListener();
    sut1.registerJobListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(((TaskLevelListener) sut1.getJobLevelListener().getWtaTaskListener()).getStageToTasks())
        .isEmpty();
    assertThat(((TaskLevelListener) sut1.getJobLevelListener().getWtaTaskListener()).getTaskToStage())
        .isEmpty();
    assertThat(sut1.getStageLevelListener().getStageToJob()).isEmpty();
    assertThat(sut1.getStageLevelListener().getStageToParents()).isEmpty();
    assertThat(sut1.getStageLevelListener().getParentStageToChildrenStages())
        .isEmpty();
    assertThat(sut1.getStageLevelListener().getStageToResource()).isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().get(0).getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }

  @Test
  void endOfJobShouldClearTheMapOfEntriesAfterJobIsDoneButNotProcessedObjectsStageLevel() {
    sut2.registerStageListener();
    sut2.registerJobListener();
    invokeJob();
    invokeJob();
    stopJob();
    assertThat(sut2.getStageLevelListener().getStageToJob()).isEmpty();
    assertThat(sut2.getStageLevelListener().getStageToParents()).isEmpty();
    assertThat(sut2.getStageLevelListener().getParentStageToChildrenStages())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getStageToResource()).isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut2.getStageLevelListener().getProcessedObjects().get(0).getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
