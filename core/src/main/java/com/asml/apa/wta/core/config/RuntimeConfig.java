package com.asml.apa.wta.core.config;

import com.asml.apa.wta.core.model.enums.Domain;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

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
  private boolean isStageLevel = false;

  @Builder.Default
  private int resourcePingInterval = defaultResourcePingInterval();

  @Builder.Default
  private int executorSynchronizationInterval = defaultExecutorSynchronizationInterval();

  private String outputPath;

  /**
   * Default resource ping interval, specified at 1000ms.
   *
   * @return 1000 as the default resource ping interval
   * @author Henry Page
   * @since 1.0.0
   */
  public static int defaultResourcePingInterval() {
    return 1000;
  }

  /**
   * Default executor synchronization interval, specified at 2000ms.
   *
   * @return 2000 as the default executor synchronization interval
   * @author Henry Page
   * @since 1.0.0
   */
  public static int defaultExecutorSynchronizationInterval() {
    return 2000;
  }
}
