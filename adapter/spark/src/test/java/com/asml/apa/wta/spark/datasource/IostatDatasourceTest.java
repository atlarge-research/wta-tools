package com.asml.apa.wta.spark.datasource;


import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import lombok.extern.slf4j.Slf4j;
import com.asml.apa.wta.spark.datasource.IostatDataSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class IostatDatasourceTest {

@Test
public void t() throws IOException, InterruptedException {
        IostatDataSource i = new IostatDataSource();
        assertTrue(i.getAllMetrics("driver") instanceof IostatDataSourceDto);
    }
}
