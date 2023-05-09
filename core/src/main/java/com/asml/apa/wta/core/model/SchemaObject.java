package com.asml.apa.wta.core.model;

import java.io.Serializable;
import lombok.experimental.SuperBuilder;

/**
 * Base WTA object class
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@SuperBuilder
public abstract class SchemaObject implements Serializable {
  private static final long serialVersionUID = 1L;
}
