package com.asml.apa.wta.core.io;

import com.asml.apa.wta.core.model.Resource;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class JsonWriterIntegrationTest {

  @Test
  void writeRecordToFile() throws IOException {
    Resource resource = Resource.builder().id(2).numResources(7.2).os("Ubuntu").diskSpace(123).procModel("Intel Xeon").build();
    OutputFile path = new DiskOutputFile(Path.of("test.json"));
    try (JsonWriter<Resource> writer = new JsonWriter<>(path)) {
      writer.write(resource);
    }
    assertThat(new File("test.json").exists()).isTrue();
  }
}