package com.asml.apa.wta.core.dto;

import java.io.Serializable;
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
public class BaseSupplierDto implements Serializable {

  private static final long serialVersionUID = 21352L;

  private long timestamp;

  @Builder.Default
  private OsInfoDto osInfoDto = null;

  @Builder.Default
  private IostatDto iostatDto = null;

  @Builder.Default
  private DstatDto dstatDto = null;

  @Builder.Default
  private PerfDto perfDto = null;

  @Builder.Default
  private JvmFileDto jvmFileDto = null;

  @Builder.Default
  private ProcDto procDto = null;
}
