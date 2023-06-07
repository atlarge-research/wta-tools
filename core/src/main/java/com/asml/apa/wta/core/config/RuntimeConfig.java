package com.asml.apa.wta.core.config;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Config class for the plugin.
 *
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @author Pil Kyu Cho
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class RuntimeConfig {

  private String[] authors;

  private Domain domain;

  @Builder.Default
  private String description = "";

  @Builder.Default
  private String logLevel = "ERROR";

  @Builder.Default
  private boolean isStageLevel = false;

  @Builder.Default
  private int resourcePingInterval = 1000;

  @Builder.Default
  private int executorSynchronizationInterval = 2000;

  private String outputPath;
}
