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
import com.asml.apa.wta.core.stream.Stream;
import java.io.IOException;
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
   * @param toolVersion the version of the tool that writes to file
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public WtaWriter(@NonNull OutputFile path, String version, String toolVersion) {
    file = path.resolve(toolVersion);
    schemaVersion = version;
  }

  /**
   * Writes a {@link Workload} to the corresponding JSON file.
   *
   * @param workload the workload to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(Workload workload) {
    log.debug("Writing workload to file.");
    try (JsonWriter<Workload> workloadWriter = createWorkloadWriter()) {
      workloadWriter.write(workload);
    } catch (IOException e) {
      log.error("Could not write workload to file.");
    }
  }

  /**
   * Writes a {@link Stream} of WTA objects to their corresponding Parquet file.
   *
   * @param clazz the class of WTA objects to write
   * @param wtaObjects the WTA objects to write
   * @param <T> type parameter for the type of WTA object to write, should extend {@link BaseTraceObject}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <T extends BaseTraceObject> void write(Class<T> clazz, Stream<T> wtaObjects) {
    log.debug("Writing objects of type {} to file.", clazz.getSimpleName());
    String label = parquetLabels.get(clazz);
    ParquetSchema schema = new ParquetSchema(clazz, wtaObjects.copy(), label);
    try {
      OutputFile path = file.resolve(label)
          .resolve(schemaVersion)
          .clearDirectories()
          .resolve(label + ".parquet");
      try (ParquetWriter<T> wtaParquetWriter = new ParquetWriter<>(path, schema)) {
        while (!wtaObjects.isEmpty()) {
          wtaParquetWriter.write(wtaObjects.head());
        }
      }
    } catch (IOException e) {
      log.error("Could not write {} to file.", label);
    }
  }

  /**
   * Creates a Workload json writer.
   *
   * @return JsonWriter a json writer that writes the workload json file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  protected JsonWriter<Workload> createWorkloadWriter() throws IOException {
    OutputFile path = file.resolve("workload")
        .resolve(schemaVersion)
        .clearDirectories()
        .resolve("generic_information.json");
    return new JsonWriter<>(path);
  }
}
