package com.asml.apa.wta.core;

import com.asml.apa.wta.core.io.JsonWriter;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.io.ParquetSchema;
import com.asml.apa.wta.core.io.ParquetWriter;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Facade over the specific writers to persists all the WTA files.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class WtaWriter implements Flushable, AutoCloseable {

  private final JsonWriter<Workload> workloadWriter;
  private final ParquetWriter<Task> taskWriter;
  private final ParquetWriter<Resource> resourceWriter;
  private final ParquetWriter<Workflow> workflowWriter;

  private final Workload workloadToWrite;

  private final List<Workflow> workflowsToWrite;

  private final List<Resource> resourcesToWrite;

  private final List<Task> tasksToWrite;

  /**
   * Sets up a WTA writer for the specified output path and version.
   *
   * @param path the output path to write to
   * @param version the version of files to write
   * @param workload the {@link Workload} to write
   * @param workflows the {@link Workflow}s to write
   * @param resources the {@link Resource}s to write
   * @param tasks the {@link Task}s to write
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public WtaWriter(
      @NonNull OutputFile path,
      String version,
      Workload workload,
      List<Workflow> workflows,
      List<Resource> resources,
      List<Task> tasks)
      throws IOException {
    setupDirectories(path, version);
    workloadToWrite = workload;
    workflowsToWrite = workflows;
    resourcesToWrite = resources;
    tasksToWrite = tasks;
    workloadWriter =
        new JsonWriter<>(path.resolve("workload").resolve(version).resolve("generic_information.json"));
    taskWriter = new ParquetWriter<>(
        path.resolve("tasks").resolve(version).resolve("task.parquet"),
        new ParquetSchema(Task.class, tasks, "tasks"));
    resourceWriter = new ParquetWriter<>(
        path.resolve("resources").resolve(version).resolve("resource.parquet"),
        new ParquetSchema(Resource.class, resources, "resources"));
    workflowWriter = new ParquetWriter<>(
        path.resolve("workflows").resolve(version).resolve("workflow.parquet"),
        new ParquetSchema(Workflow.class, workflows, "workflows"));
  }

  /**
   * Write all the objects to file.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write() {
    try {
      workloadWriter.write(workloadToWrite);
      for (Workflow workflow : workflowsToWrite) {
        workflowWriter.write(workflow);
      }
      for (Resource resource : resourcesToWrite) {
        resourceWriter.write(resource);
      }
      for (Task task : tasksToWrite) {
        taskWriter.write(task);
      }
      flush();
    } catch (Exception e) {
      log.error("Could not write all WTA files.");
    }
  }

  /**
   * Flushes the objects to file.
   *
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void flush() throws IOException {
    workloadWriter.flush();
  }

  /**
   * Prepares the system for writing.
   * Deletes old files in the output folder and initialises the directory structure.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  private void setupDirectories(OutputFile path, String version) {
    try {
      path.resolve("workload").resolve(version).resolve(".temp").clearDirectory();
      path.resolve("workflows").resolve(version).resolve(".temp").clearDirectory();
      path.resolve("tasks").resolve(version).resolve(".temp").clearDirectory();
      path.resolve("resources").resolve(version).resolve(".temp").clearDirectory();
    } catch (IOException e) {
      log.error("Could not create directory structure for the output.");
    }
  }

  /**
   * Closes the writer.
   *
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public void close() throws IOException {
    resourceWriter.close();
    taskWriter.close();
    workflowWriter.close();
    workloadWriter.close();
  }
}
