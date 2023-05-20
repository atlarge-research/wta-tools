package com.asml.apa.wta.core.config;

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

  private String author;

  private String domain;

  @Builder.Default
  private String description = "";

  @Builder.Default
  private Map<String, String> events = new HashMap<>();

  @Builder.Default
  private String logLevel = "ERROR";

  @Builder.Default
  private boolean doConsoleLog = true;

  @Builder.Default
  private boolean doFileLog = true;
}
