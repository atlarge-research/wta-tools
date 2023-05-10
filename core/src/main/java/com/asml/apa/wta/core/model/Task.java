package com.asml.apa.wta.core.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

/**
 * Task class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class Task extends SchemaObject {
  private static final long serialVersionUID = 3L;
}
