package com.asml.apa.wta.core.dto;

import com.asml.apa.wta.core.supplier.IostatSupplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data transfer object for the {@link IostatSupplier}.
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

}
