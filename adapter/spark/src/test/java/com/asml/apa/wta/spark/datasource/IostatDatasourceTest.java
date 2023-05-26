package com.asml.apa.wta.spark.datasource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class IostatDatasourceTest {

  @Test
  public void getAllMetricsReturnsIostatDto() throws IOException, InterruptedException {
    IostatDataSource i = new IostatDataSource();
    assertTrue(i.getAllMetrics("driver") instanceof IostatDataSourceDto);
  }
}
