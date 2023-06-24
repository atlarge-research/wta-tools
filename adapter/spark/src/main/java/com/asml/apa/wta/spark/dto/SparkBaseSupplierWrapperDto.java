package com.asml.apa.wta.spark.dto;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * A wrapper for {@link BaseSupplierDto} that contains additional Spark related information.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class SparkBaseSupplierWrapperDto extends BaseSupplierDto {

  private static final long serialVersionUID = -7992568771408736130L;

  private String executorId;
}
