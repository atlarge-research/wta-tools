package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.stream.KeyedStream;
import com.asml.apa.wta.core.stream.Stream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * This abstract class is a base class for the task-level and stage-level listeners.
 *
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public abstract class TaskStageBaseListener extends AbstractListener<Task> {

  @Getter
  private final Map<Long, Long> stageToJob = new ConcurrentHashMap<>();

  @Getter
  private final KeyedStream<Long, Task> workflowsToTasks = new KeyedStream<>();

  /**
   * Constructor for the stage-level listener.
   *
   * @param sparkContext        current spark context
   * @param config              additional config specified by the user for the plugin
   */
  public TaskStageBaseListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method is called every time a job starts. In the context of the WTA, this is a workflow.
   *
   * @param jobStart            SparkListenerJobStart object corresponding to information on job start
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    long jobId = jobStart.jobId() + 1;
    jobStart.stageInfos().foreach(stageInfo -> stageToJob.put((long) stageInfo.stageId() + 1, jobId));
  }

  /**
   * Associates a {@link Task} with a {@link Workflow}.
   * Also adds the {@link Task} to the processed objects {@link Stream}.
   *
   * @param workflowId          id of the {@link Workflow} to add the {@link Task} to
   * @param task                {@link Task} to add
   */
  public void addTaskToWorkflow(long workflowId, Task task) {
    workflowsToTasks.addToStream(workflowId, task);
  }
}
