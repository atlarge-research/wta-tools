package com.asml.apa.wta.core.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuntimeConfig {

  private String author;

  private String domain;

  @Builder.Default
  private String description = "";
}
