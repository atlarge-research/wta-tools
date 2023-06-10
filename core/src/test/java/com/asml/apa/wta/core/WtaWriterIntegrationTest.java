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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WtaWriterIntegrationTest {

  private static WtaWriter sut;

  @BeforeAll
  static void setUp() {
    OutputFile file = new DiskOutputFile(Path.of("wta-output"));
    sut = new WtaWriter(file, "schema-1.0");
  }

  @Test
  void emptyOutputFile() {
    assertThatThrownBy(() -> new WtaWriter(null, "schema-1.0"));
  }

  @Test
  void writeWorkload() {
    Workload workload = Workload.builder().build();
    sut.write(workload);
    assertThat(new File("wta-output/workload/schema-1.0/generic_information.json").exists())
        .isTrue();
  }

  @Test
  void writeWorkflows() {
    Workflow workflow = Workflow.builder().build();
    sut.write(Workflow.class, List.of(workflow));
    assertThat(new File("wta-output/workflow/schema-1.0/workflow.parquet").exists())
        .isTrue();
  }

  @Test
  void writeTasks() {
    Task task = Task.builder().build();
    sut.write(Task.class, List.of(task));
    assertThat(new File("wta-output/task/schema-1.0/task.parquet").exists()).isTrue();
  }

  @Test
  void writeResources() {
    Resource resource = Resource.builder().build();
    sut.write(Resource.class, List.of(resource));
    assertThat(new File("wta-output/resource/schema-1.0/resource.parquet").exists())
        .isTrue();
  }

  @Test
  void writeResourceStates() {
    ResourceState resourceState = ResourceState.builder().build();
    sut.write(ResourceState.class, List.of(resourceState));
    assertThat(new File("wta-output/resource_state/schema-1.0/resource_state.parquet").exists())
        .isTrue();
  }

  @AfterAll
  static void cleanUp() throws IOException {
    try (Stream<Path> stream = Files.walk(Path.of("wta-output"))) {
      stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }
}
