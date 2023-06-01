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
/**
 * Config class for the plugin.
 *
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @author Pil Kyu Cho
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class RuntimeConfig {

  private String[] authors;

  private Domain domain;

  @Builder.Default
  private String description = "";

  @Builder.Default
  private Map<String, String> events = new HashMap<>();

  @Builder.Default
  private String logLevel = "ERROR";

  @Builder.Default
  private int resourcePingInterval = 1000;

  @Builder.Default
  private int executorSynchronizationInterval = 2000;

  private String outputPath;
}
