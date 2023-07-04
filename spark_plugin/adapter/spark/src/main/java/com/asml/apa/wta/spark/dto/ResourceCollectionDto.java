package com.asml.apa.wta.spark.dto;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A value class that represents a list of ping information coming in from the executor.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResourceCollectionDto implements Serializable {

  private static final long serialVersionUID = 23478789234L;

  private List<SparkBaseSupplierWrapperDto> resourceCollection;
}
