package com.asml.apa.wta.core;

import com.asml.apa.wta.core.io.DiskJsonWriter;
import com.asml.apa.wta.core.io.DiskParquetWriter;
import com.asml.apa.wta.core.io.JsonWriter;
import com.asml.apa.wta.core.io.ParquetWriter;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;

/**
 * Persists the WTA files.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class WtaWriter {

  private final String schemaVersion;

  private Workload workloadToWrite = null;
  private final Collection<Workflow> workflowsToWrite = new ArrayList<>();
  private final Collection<Resource> resourcesToWrite = new ArrayList<>();
  private final Collection<Task> tasksToWrite = new ArrayList<>();

  private final JsonWriter<Workload> workloadWriter;
  private final ParquetWriter<Task> taskWriter;
  private final ParquetWriter<Resource> resourceWriter;
  private final ParquetWriter<Workflow> workflowWriter;

  /**
   * Sets up a WTA writer for the specified output path and version.
   *
   * @param path the output path to write to
   * @param version the version of files to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public WtaWriter(Path path, String version) {
    schemaVersion = version;

    workloadWriter =
        new DiskJsonWriter<>(path.resolve("workload").resolve(version).resolve("generic_information.json"));
    taskWriter =
        new DiskParquetWriter<>(path.resolve("tasks").resolve(version).resolve("tasks.parquet"));
    resourceWriter = new DiskParquetWriter<>(
        path.resolve("resources").resolve(version).resolve("resources.parquet"));
    workflowWriter = new DiskParquetWriter<>(
        path.resolve("workflows").resolve(version).resolve("workflows.parquet"));
  }

  public void add(Workload workload) {
    workloadToWrite = workload;
  }

  public void add(Workflow workflow) {
    workflowsToWrite.add(workflow);
  }

  public void add(Resource resource) {
    resourcesToWrite.add(resource);
  }

  public void add(Task task) {
    tasksToWrite.add(task);
  }

  /**
   * Writes the objects to file.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void writeToFile() {
    writeWorkload();
    writeWorkflows();
    writeResources();
    writeTasks();
  }

  private void writeWorkload() {}

  private void writeWorkflows() {}

  private void writeResources() {}

  private void writeTasks() {}

  /**
   * Prepares the system for writing.
   * Deletes old files in the output folder and initialises the directory structure.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void prepareForWrite() {}
}
