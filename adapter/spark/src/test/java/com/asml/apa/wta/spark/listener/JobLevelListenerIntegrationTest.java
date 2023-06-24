package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.spark.BaseSparkJobIntegrationTest;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class JobLevelListenerIntegrationTest extends BaseSparkJobIntegrationTest {

  @Test
  void getJobMetricsHasJobsAfterSparkJobAndYieldsNoErrors() throws InterruptedException {
    sut1.registerTaskListener();
    sut1.registerStageListener();
    sut1.registerJobListener();
    invokeJob();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut1.getJobLevelListener().getJobSubmitTimes()).isEmpty();
    assertThat(sut1.getJobLevelListener().getProcessedObjects().isEmpty()).isFalse();

    Workflow workflow =
        sut1.getJobLevelListener().getProcessedObjects().head();
    assertThat(workflow.getId()).isGreaterThan(0L);
    assertThat(workflow.getTsSubmit()).isGreaterThan(0L);
    assertThat(workflow.getApplicationName())
        .isEqualTo(spark.sparkContext().getConf().get("spark.app.name"));
    assertThat(workflow.getScheduler()).isEqualTo("FIFO");

    assertThat(workflow.getTaskCount()).isGreaterThan(0L);
    assertThat(sut1.getJobLevelListener().getProcessedObjects().toList())
        .hasSize(2)
        .isSortedAccordingTo(Comparator.comparing(Workflow::getTsSubmit));
  }

  @Test
  void jobsHaveNoTasksIfTaskListenerNotInvoked() throws InterruptedException {
    sut1.registerJobListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut1.getJobLevelListener().getJobSubmitTimes()).isEmpty();
    assertThat(sut1.getJobLevelListener().containsProcessedObjects()).isTrue();

    assertThat(sut1.getJobLevelListener().getProcessedObjects().toList())
        .hasSizeGreaterThan(0)
        .allMatch(wf -> wf.getTaskCount() == 0);
  }

  @Test
  void testSparkTaskProcessedObjects() throws InterruptedException {
    sut1.registerTaskListener();
    sut1.registerStageListener();
    sut1.registerJobListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut1.getTaskLevelListener().getProcessedObjects().toList()).hasSizeGreaterThan(0);
    assertThat(sut1.getStageLevelListener().getProcessedObjects().toList()).hasSizeGreaterThan(0);
    assertThat(sut1.getJobLevelListener().getProcessedObjects().toList()).hasSizeGreaterThan(0);
  }

  @Test
  void testSparkStageProcessedObjects() throws InterruptedException {
    sut2.registerStageListener();
    sut2.registerJobListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut2.getTaskLevelListener().getProcessedObjects().toList()).hasSize(0);
    assertThat(sut2.getStageLevelListener().getProcessedObjects().toList()).hasSizeGreaterThan(0);
    assertThat(sut2.getJobLevelListener().getProcessedObjects().toList()).hasSizeGreaterThan(0);
  }

  @Test
  void endOfJobShouldClearTheMapOfEntriesAfterJobIsDoneButNotProcessedObjectsTaskLevel() throws InterruptedException {
    sut1.registerTaskListener();
    sut1.registerStageListener();
    sut1.registerJobListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(((TaskLevelListener) sut1.getJobLevelListener().getWtaTaskListener()).getStageToTasks())
        .isEmpty();
    assertThat(((TaskLevelListener) sut1.getJobLevelListener().getWtaTaskListener()).getTaskToStage())
        .isEmpty();
    assertThat(sut1.getStageLevelListener().getStageToJob()).isEmpty();
    assertThat(sut1.getStageLevelListener().getStageToParents()).isEmpty();
    assertThat(sut1.getStageLevelListener().getParentStageToChildrenStages())
        .isEmpty();
    assertThat(sut1.getStageLevelListener().getStageToResource()).isEmpty();
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().toList()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut1.getTaskLevelListener().getProcessedObjects().head().getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }

  @Test
  void endOfJobShouldClearTheMapOfEntriesAfterJobIsDoneButNotProcessedObjectsStageLevel() throws InterruptedException {
    sut2.registerStageListener();
    sut2.registerJobListener();
    invokeJob();
    stopJob();
    AbstractListener.getThreadPool().awaitTermination(1, TimeUnit.SECONDS);

    assertThat(sut2.getStageLevelListener().getStageToJob()).isEmpty();
    assertThat(sut2.getStageLevelListener().getStageToParents()).isEmpty();
    assertThat(sut2.getStageLevelListener().getParentStageToChildrenStages())
        .isEmpty();
    assertThat(sut2.getStageLevelListener().getStageToResource()).isEmpty();
    assertThat(sut2.getStageLevelListener().getProcessedObjects().toList()).hasSizeGreaterThanOrEqualTo(1);
    assertThat(sut2.getStageLevelListener().getProcessedObjects().head().getWorkflowId())
        .isInstanceOf(Long.class)
        .isNotNull();
  }
}
