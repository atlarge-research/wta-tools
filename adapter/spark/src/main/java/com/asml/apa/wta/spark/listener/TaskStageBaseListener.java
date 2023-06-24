package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.streams.KeyedStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * This abstract class is a base class for the task and stage level listeners.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@SuppressWarnings("VisibilityModifier")
public abstract class TaskStageBaseListener extends AbstractListener<Task> {

  @Getter
  private final Map<Long, Long> stageToJob = new ConcurrentHashMap<>();

  @Getter
  private final KeyedStream<Long, Task> workflowsToTasks = new KeyedStream<>();

  /**
   * Constructor for the stage-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  protected TaskStageBaseListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method is called every time a job starts. In the context of the WTA, this is a workflow.
   *
   * @param jobStart The object corresponding to information on job start.
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    long jobId = jobStart.jobId() + 1;
    jobStart.stageInfos().foreach(stageInfo -> stageToJob.put((long) stageInfo.stageId() + 1, jobId));
  }

  /**
   * Removes the {@link Task}s associated with the {@link com.asml.apa.wta.core.model.Workflow} from the
   * {@link KeyedStream}.
   *
   * @param workflowId the id of the {@link com.asml.apa.wta.core.model.Workflow} to clear
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void removesWorkflowAssociations(long workflowId) {
    workflowsToTasks.dropKey(workflowId);
  }

  /**
   * Associates a {@link Task} with a {@link com.asml.apa.wta.core.model.Workflow}.
   * Also adds the {@link Task} to the processed objects {@link com.asml.apa.wta.core.streams.Stream}.
   *
   * @param workflowId the id of the {@link com.asml.apa.wta.core.model.Workflow} to add the {@link Task} to
   * @param task the {@link Task} to add
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addTaskToWorkflow(long workflowId, Task task) {
    workflowsToTasks.addToStream(workflowId, task);
    addProcessedObject(task);
  }
}
