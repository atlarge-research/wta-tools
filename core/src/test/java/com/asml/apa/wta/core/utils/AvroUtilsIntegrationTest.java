package com.asml.apa.wta.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroUtilsIntegrationTest {

  private AvroUtils utils;
  private Schema schema;
  private File path;

  @BeforeEach
  void inits() {
    schema = SchemaBuilder.record("resource")
        .namespace("com.asml.apa.wta.core.model")
        .fields()
        .name("id")
        .type()
        .longType()
        .noDefault()
        .name("type")
        .type()
        .nullable()
        .stringType()
        .stringDefault("test")
        .name("numResources")
        .type()
        .doubleType()
        .doubleDefault(0.0)
        .name("procModel")
        .type()
        .nullable()
        .stringType()
        .stringDefault("test")
        .name("memory")
        .type()
        .longType()
        .longDefault(0)
        .name("diskSpace")
        .type()
        .longType()
        .longDefault(0)
        .name("networkSpeed")
        .type()
        .longType()
        .longDefault(0)
        .name("os")
        .type()
        .nullable()
        .stringType()
        .stringDefault("test")
        .name("details")
        .type()
        .nullable()
        .stringType()
        .stringDefault("test")
        .endRecord();
    path = new File("./src/test/resources/AvroOutput");
    new File(path, "/writeRecords").delete();
    new File(path, "/outputUri").delete();
    new File(path, "/schema").delete();
  }

  @Test
  void writeRecordsTest() {
    Resource resource = Resource.builder().build();
    GenericData.Record record = new GenericData.Record(schema);
    record.put("id", resource.getId());
    record.put("type", resource.getType());
    record.put("numResources", resource.getNumResources());
    record.put("procModel", resource.getProcModel());
    record.put("memory", resource.getMemory());
    record.put("diskSpace", resource.getDiskSpace());
    record.put("networkSpeed", resource.getNetworkSpeed());
    record.put("os", resource.getOs());
    record.put("details", resource.getDetails());
    List<GenericRecord> recordList = new ArrayList<>();
    recordList.add(record);
    Assertions.assertDoesNotThrow(() -> {
      utils = new AvroUtils(schema, new File(path, "/writeRecords"));
      utils.writeRecords(recordList);
    });
  }

  @Test
  void getOutputUriTest() throws Exception {
    utils = new AvroUtils(schema, new File(path, "/outputUri"));
    assertThat(utils.getUri())
        .isEqualTo(new File(path, "/outputUri").toURI().getPath());
  }

  @Test
  void getAvroSchemaTest() throws Exception {
    utils = new AvroUtils(schema, new File(path, "/schema"));
    assertThat(utils.getAvroSchema()).isEqualTo(schema);
  }
}
