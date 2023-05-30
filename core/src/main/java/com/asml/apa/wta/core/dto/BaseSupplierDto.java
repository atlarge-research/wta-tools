package com.asml.apa.wta.core.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
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

  private static final long serialVersionUID = -3101218638564306099L;

  private OsInfoDto osInfoDto;

  private IostatDto iostatDto;
}
