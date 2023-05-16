package com.asml.apa.wta.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParquetWriterUtilsTest {

  private Resource resource;
  private ParquetWriterUtils utils;
  List<Resource> resources;

  @BeforeEach
  void init() {
    var resourceBuilder = Resource.builder()
        .id(1)
        .type("test")
        .os("test os")
        .details("None")
        .diskSpace(2)
        .numResources(4.0)
        .memory(8)
        .networkSpeed(16)
        .procModel("test model");
    resource = resourceBuilder.build();
    resources = new ArrayList<>();
    utils = new ParquetWriterUtils(new File("./WTA"), "schema-1.0");
  }

  @Test
  void readResourceTest() {
    resources.add(resource);
    utils.readResource(resource);
    assertThat(resources).isEqualTo(utils.getResources());
  }

  @Test
  void writeToFileTest() {
    resources.add(resource);
    utils.readResource(resource);
    try {
      utils.writeToFile("test1", "test2", "test3", "test4");
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }
}
