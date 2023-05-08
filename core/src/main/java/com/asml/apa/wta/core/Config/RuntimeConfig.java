package com.asml.apa.wta.core.Config;

import javax.validation.constraints.NotBlank;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuntimeConfig {

  @NotBlank(message = "Author is mandatory")
  private String author;

  @NotBlank(message = "Domain is mandatory")
  private String domain;

  @Builder.Default
  private String description = "";
}
