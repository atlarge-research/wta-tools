package com.asml.apa.wta.core.model.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Domain enum
 *
 * @author Lohithsai Yadala Chanchu
 * @version 1.0.0
 */
@AllArgsConstructor
public enum Domain {
  BIOMEDICAL("BIOMEDICAL"),
  ENGINEERING("ENGINEERING"),
  INDUSTRIAL("INDUSTRIAL"),
  SCIENTIFIC("SCIENTIFIC");

  @Getter
  private String value;
}
