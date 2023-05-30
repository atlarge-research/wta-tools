package com.asml.apa.wta.spark.dto;

import com.asml.apa.wta.core.dto.BigSupplierDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class SparkSupplierDto extends BigSupplierDto {

  private String executorId;
}
