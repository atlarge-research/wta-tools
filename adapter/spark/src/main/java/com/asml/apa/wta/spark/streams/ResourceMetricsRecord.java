package com.asml.apa.wta.spark.streams;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Resource metrics record.
 *
 * @author Atour Mousavi Gourabi
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Getter
@AllArgsConstructor
public class ResourceMetricsRecord implements Serializable {

  private String executorId;
}
