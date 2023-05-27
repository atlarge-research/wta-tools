package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.dto.OperatingSystemDataSourceDto;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Resource metrics record.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
@AllArgsConstructor
public class ResourceMetricsRecord implements Serializable {

  private static final long serialVersionUID = -3101218638564306099L;

  private OperatingSystemDataSourceDto operatingSystemDataSourceDto;

  private String executorId;
}
