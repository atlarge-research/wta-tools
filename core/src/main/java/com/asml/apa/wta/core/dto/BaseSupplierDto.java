package com.asml.apa.wta.core.dto;

import java.io.Serializable;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Contains information that can be gathered regardless of the application.
 * Adapters can extend this Dto with additional application-specific information.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class BaseSupplierDto implements Serializable {

  private static final long serialVersionUID = 21352L;

  private long timestamp;

  @Builder.Default
  private Optional<OsInfoDto> osInfoDto = Optional.empty();

  @Builder.Default
  private Optional<IostatDto> iostatDto = Optional.empty();

  @Builder.Default
  private Optional<DstatDto> dstatDto = Optional.empty();

  @Builder.Default
  private Optional<PerfDto> perfDto = Optional.empty();

  @Builder.Default
  private Optional<JvmFileDto> jvmFileDto = Optional.empty();

  @Builder.Default
  private Optional<ProcDto> procDto = Optional.empty();
}
