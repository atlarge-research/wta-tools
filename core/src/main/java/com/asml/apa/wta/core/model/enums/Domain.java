package com.asml.apa.wta.core.model.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Domain enum for WTA traces.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@AllArgsConstructor
public enum Domain {
  BIOMEDICAL("Biomedical"),
  ENGINEERING("Engineering"),
  INDUSTRIAL("Industrial"),
  SCIENTIFIC("Scientific");

  @Getter
  private String value;
}
