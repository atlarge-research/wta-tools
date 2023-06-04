package com.asml.apa.wta.spark.dto;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
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
public class SparkBaseSupplierWrapperDto extends BaseSupplierDto {

  private String executorId;
}
