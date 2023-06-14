package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

/**
 * This class is an application-level listener for the Spark data source.
 * It's important that one does not override {@link org.apache.spark.scheduler.SparkListenerInterface#onApplicationStart(SparkListenerApplicationStart)} here, as the event is already sent
 * before the listener is registered unless the listener is explicitly registered in the Spark configuration as per <a href="https://stackoverflow.com/questions/36401238/spark-onapplicationstart-is-never-gets-called">SO</a>
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
@Slf4j
public class ApplicationLevelListener extends AbstractListener<Workload> {

  private final JobLevelListener jobLevelListener;

  private final TaskLevelListener taskLevelListener;

  private final StageLevelListener stageLevelListener;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param jobLevelListener The job-level listener to be used by this listener
   * @param taskLevelListener The task-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener to be used by this listener
   * @author Henry Page
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      JobLevelListener jobLevelListener,
      TaskLevelListener taskLevelListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.jobLevelListener = jobLevelListener;
    this.taskLevelListener = taskLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Callback function that is called right at the end of the application. Further experimentation
   * is needed to determine if applicationEnd is called first or shutdown.
   *
   * @param applicationEnd The event corresponding to the end of the application
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    // we should never enter this branch, this is a guard since an application only terminates once.
    List<Workload> processedObjects = this.getProcessedObjects();
    if (!processedObjects.isEmpty()) {
      log.debug("Application end called twice, this should never happen");
      return;
    }

    final Workflow[] workflows = jobLevelListener.getProcessedObjects().toArray(new Workflow[0]);
    final int numWorkflows = workflows.length;
    final int totalTasks =
        Arrays.stream(workflows).mapToInt(Workflow::getTaskCount).sum();
    final Domain domain = config.getDomain();
    final long startDate = sparkContext.startTime();
    final long endDate = applicationEnd.time();
    final String[] authors = config.getAuthors();
    final String workloadDescription = config.getDescription();

    if (config.isStageLevel()) {
      addStageRelations();
    } else {
      addTaskRelations();
    }

    // unknown
    final long numSites = -1L;
    final long numResources = -1L;
    final long numUsers = -1L;
    final long numGroups = -1L;
    final double totalResourceSeconds = -1.0;

    // all statistics (stdev, mean, etc.) are unknown
    processedObjects.add(Workload.builder()
        .totalWorkflows(numWorkflows)
        .totalTasks(totalTasks)
        .domain(domain)
        .dateStart(startDate)
        .dateEnd(endDate)
        .authors(authors)
        .workloadDescription(workloadDescription)
        .numSites(numSites)
        .numResources(numResources)
        .numUsers(numUsers)
        .numGroups(numGroups)
        .totalResourceSeconds(totalResourceSeconds)
        .build());
  }

  /**
   * Goes over all the processed Spark tasks and add its parent and child tasks, if they exist.
   *
   * @author Tianchen QU
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private void addTaskRelations() {
    for (Task task : taskLevelListener.getProcessedObjects()) {
      final int stageId = taskLevelListener.getTaskToStage().get(task.getId());
      final Integer[] parentStages =
          stageLevelListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        task.setParents(Arrays.stream(parentStages)
            .flatMap(parentStageId -> Arrays.stream(taskLevelListener
                .getStageToTasks()
                .getOrDefault(parentStageId, new ArrayList<>())
                .toArray(new Long[0])))
            .mapToLong(Long::longValue)
            .toArray());
      }
      List<Integer> childrenStages =
          stageLevelListener.getParentToChildren().get(stageId);
      if (childrenStages != null) {
        task.setChildren(childrenStages.stream()
            .flatMap(childStageId -> taskLevelListener.getStageToTasks().get(childStageId).stream())
            .mapToLong(Long::longValue)
            .toArray());
      }
    }
  }

  /**
   * Goes over all the processed Spark stages and add its parent and child stages, if they exist.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private void addStageRelations() {
    for (Task stage : stageLevelListener.getProcessedObjects()) {
      final int stageId = (int) stage.getId();
      final Integer[] parentStages =
          stageLevelListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        stage.setParents(Arrays.stream(parentStages)
            .mapToLong(Integer::longValue)
            .toArray());
      }
      final List<Integer> childrenStages =
          stageLevelListener.getParentToChildren().get(stageId);
      if (childrenStages != null) {
        stage.setChildren(
            childrenStages.stream().mapToLong(Integer::longValue).toArray());
      }
    }
  }
}
