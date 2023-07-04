package com.asml.apa.wta.core.dto;

import com.asml.apa.wta.core.supplier.PerfSupplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data transfer object for the {@link PerfSupplier}.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PerfDto implements SupplierDto {

  private static final long serialVersionUID = 2352354356342423L;

  private double watt;
}
