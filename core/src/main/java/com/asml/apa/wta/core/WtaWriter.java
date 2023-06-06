package com.asml.apa.wta.core;

import com.asml.apa.wta.core.io.JsonWriter;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.io.ParquetWriter;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import java.io.Flushable;
import java.io.IOException;
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

  /**
   * Sets up a WTA writer for the specified output path and version.
   *
   * @param path the output path to write to
   * @param version the version of files to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public WtaWriter(OutputFile path, String version) throws Exception {
    setupDirectories(path, version);
    workloadWriter =
        new JsonWriter<>(path.resolve("workflow").resolve(version).open());
    taskWriter = new ParquetWriter<>(path.resolve("tasks").resolve(version).open());
    resourceWriter =
        new ParquetWriter<>(path.resolve("resources").resolve(version).open());
    workflowWriter =
        new ParquetWriter<>(path.resolve("workflows").resolve(version).open());
  }

  public void write(Workload workload) {
    try {
      workloadWriter.write(workload);
    } catch (Exception e) {
      log.error("Could not write workload {}.", workload);
    }
  }

  public void write(Workflow workflow) {
    try {
      workflowWriter.write(workflow);
    } catch (Exception e) {
      log.error("Could not write workflow {}.", workflow);
    }
  }

  public void write(Resource resource) {
    try {
      resourceWriter.write(resource);
    } catch (Exception e) {
      log.error("Could not write resource {}.", resource);
    }
  }

  public void write(Task task) {
    try {
      taskWriter.write(task);
    } catch (Exception e) {
      log.error("Could not write task {}.", task);
    }
  }

  /**
   * Flushes the objects to file.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void flush() throws IOException {
    taskWriter.flush();
    workloadWriter.flush();
    workflowWriter.flush();
    resourceWriter.flush();
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
      path.resolve("workflow").resolve(version).clearDirectory();
      path.resolve("workloads").resolve(version).clearDirectory();
      path.resolve("tasks").resolve(version).clearDirectory();
      path.resolve("resources").resolve(version).clearDirectory();
    } catch (IOException e) {
      log.error("Could not create directory structure for the output.");
    }
  }

  @Override
  public void close() throws Exception {
    taskWriter.close();
    workloadWriter.close();
    workflowWriter.close();
    resourceWriter.close();
  }
}
