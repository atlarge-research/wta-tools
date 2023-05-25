package com.asml.apa.wta.spark.streams;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Resource key value object.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class ResourceKey {
  private final String executorId;
}
