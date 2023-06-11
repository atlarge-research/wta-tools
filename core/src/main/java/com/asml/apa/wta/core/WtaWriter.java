package com.asml.apa.wta.core;

import com.asml.apa.wta.core.io.JsonWriter;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.io.ParquetSchema;
import com.asml.apa.wta.core.io.ParquetWriter;
import com.asml.apa.wta.core.model.BaseTraceObject;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Facade over the specific writers to persists all the WTA files.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class WtaWriter {

  private final OutputFile file;
  private final String schemaVersion;
  private final Map<Class<? extends BaseTraceObject>, String> parquetLabels = Map.of(
      Resource.class, "resources",
      ResourceState.class, "resource_states",
      Task.class, "tasks",
      Workflow.class, "workflows");

  /**
   * Sets up a WTA writer for the specified output path and version.
   *
   * @param path the output path to write to
   * @param version the version of files to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public WtaWriter(@NonNull OutputFile path, String version) {
    file = path;
    schemaVersion = version;
    setupDirectories(path, version);
  }

  /**
   * Writes a {@link Workload} to the corresponding JSON file.
   *
   * @param workload the workload to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(Workload workload) {
    OutputFile path = file.resolve("workload").resolve(schemaVersion).resolve("generic_information.json");
    try (JsonWriter<Workload> workloadWriter = new JsonWriter<>(path)) {
      workloadWriter.write(workload);
    } catch (IOException e) {
      log.error("Could not write workload to file.");
    }
  }

  /**
   * Writes a {@link List} of WTA objects to their corresponding Parquet file.
   *
   * @param clazz the class of WTA objects to write
   * @param wtaObjects the WTA objects to write
   * @param <T> type parameter for the type of WTA object to write, should extend {@link BaseTraceObject}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <T extends BaseTraceObject> void write(Class<T> clazz, List<T> wtaObjects) {
    String label = parquetLabels.get(clazz);
    ParquetSchema schema = new ParquetSchema(clazz, wtaObjects, label);
    OutputFile path = file.resolve(label).resolve(schemaVersion).resolve(label + ".parquet");
    try (ParquetWriter<T> wtaParquetWriter = new ParquetWriter<>(path, schema)) {
      for (T wtaObject : wtaObjects) {
        wtaParquetWriter.write(wtaObject);
      }
    } catch (IOException e) {
      log.error("Could not write {} to file.", label);
    }
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
      path.resolve("workflow").resolve(version).resolve(".temp").clearDirectory();
      path.resolve("task").resolve(version).resolve(".temp").clearDirectory();
      path.resolve("resource").resolve(version).resolve(".temp").clearDirectory();
      path.resolve("resource_state").resolve(version).resolve(".temp").clearDirectory();
    } catch (IOException e) {
      log.error("Could not create directory structure for the output.");
    }
  }
}
