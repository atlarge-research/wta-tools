package com.asml.apa.wta.core.dto;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * IostatDataSourceDto class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IostatDto implements SupplierDto {
  private static final long serialVersionUID = 4386177879327585527L;

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
