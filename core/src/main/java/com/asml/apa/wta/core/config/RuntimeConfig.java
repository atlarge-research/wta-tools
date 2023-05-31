package com.asml.apa.wta.core.config;

import com.asml.apa.wta.core.model.enums.Domain;
import java.util.HashMap;
import java.util.Map;
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

  private String[] authors;

  private Domain domain;

  @Builder.Default
  private String description = "";

  @Builder.Default
  private Map<String, String> events = new HashMap<>();

  @Builder.Default
  private String logLevel = "ERROR";

  private String outputPath;
}
