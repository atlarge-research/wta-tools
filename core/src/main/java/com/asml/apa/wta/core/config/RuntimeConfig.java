package com.asml.apa.wta.core.config;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
  private String logLevel = "INFO";

  @Builder.Default
  private boolean doConsoleLog = true;

  @Builder.Default
  private boolean doFileLog = true;

  @Builder.Default
  private String logPath =
      "logs/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".log";
}
