package com.asml.apa.wta.spark.dto;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.streams.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Encapsulates a given resource and its states together for further processing.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ResourceAndStateWrapper {

  private Resource resource;

  private Stream<ResourceState> states;
}
