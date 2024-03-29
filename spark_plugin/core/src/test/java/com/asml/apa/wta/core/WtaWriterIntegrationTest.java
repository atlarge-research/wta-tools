package com.asml.apa.wta.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.asml.apa.wta.core.io.DiskOutputFile;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.stream.Stream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WtaWriterIntegrationTest {

  private static final String TOOL_VERSION = "spark-wta-generator-1_0";

  private static WtaWriter sut;

  private static final String currentTime = String.valueOf(System.currentTimeMillis());

  @BeforeAll
  static void setUp() {
    OutputFile file = new DiskOutputFile(Path.of("wta-output"));
    sut = new WtaWriter(file, "schema-1.0", currentTime, TOOL_VERSION);
  }

  @Test
  void emptyOutputFile() {
    assertThatThrownBy(() -> new WtaWriter(null, "schema-1.0", currentTime, TOOL_VERSION));
  }

  @Test
  void writeWorkload() {
    Workload workload = Workload.builder().build();
    sut.write(workload);
    assertThat(new File("wta-output/" + currentTime + "/" + TOOL_VERSION
                + "/workload/schema-1.0/generic_information.json")
            .exists())
        .isTrue();
  }

  @Test
  void writeWorkflows() {
    Workflow workflow = Workflow.builder().build();
    sut.write(Workflow.class, new Stream<>(workflow));
    assertThat(new File("wta-output/" + currentTime + "/" + TOOL_VERSION
                + "/workflows/schema-1.0/workflows.parquet")
            .exists())
        .isTrue();
  }

  @Test
  void writeTasks() {
    Task task = Task.builder().build();
    sut.write(Task.class, new Stream<>(task));
    assertThat(new File("wta-output/" + currentTime + "/" + TOOL_VERSION + "/tasks/schema-1.0/tasks.parquet")
            .exists())
        .isTrue();
  }

  @Test
  void writeResources() {
    Resource resource = Resource.builder().build();
    sut.write(Resource.class, new Stream<>(resource));
    assertThat(new File("wta-output/" + currentTime + "/" + TOOL_VERSION
                + "/resources/schema-1.0/resources.parquet")
            .exists())
        .isTrue();
  }

  @Test
  void writeResourceStates() {
    ResourceState resourceState = ResourceState.builder().build();
    sut.write(ResourceState.class, new Stream<>(resourceState));
    assertThat(new File("wta-output/" + currentTime + "/" + TOOL_VERSION
                + "/resource_states/schema-1.0/resource_states.parquet")
            .exists())
        .isTrue();
  }

  @AfterAll
  static void cleanUp() throws IOException {
    Files.walk(Path.of("wta-output"))
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }
}
