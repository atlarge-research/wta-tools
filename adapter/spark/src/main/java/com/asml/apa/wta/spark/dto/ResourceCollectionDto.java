package com.asml.apa.wta.spark.dto;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResourceCollectionDto implements Serializable {

  private static final long serialVersionUID = 23478789234L;

  private List<SparkBaseSupplierWrapperDto> resourceCollection;
}
