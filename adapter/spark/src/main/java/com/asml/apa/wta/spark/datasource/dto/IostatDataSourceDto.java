package com.asml.apa.wta.spark.datasource.dto;

import com.asml.apa.wta.spark.streams.ResourceMetricsRecord;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IostatDataSourceDto extends ResourceMetricsRecord {
  private double tps;
  private double kiloByteReadPerSec;
  private double kiloByteWrtnPerSec;
  private double kiloByteDscdPerSec;
  private double kiloByteRead;
  private double kiloByteWrtn;
  private double kiloByteDscd;
  private String executorId;

  @Builder.Default
  private long timestamp = Instant.now().toEpochMilli();
}
