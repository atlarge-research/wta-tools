package com.asml.apa.wta.core.model.enums;

import lombok.AllArgsConstructor;

/**
 * Domain enum for WTA traces.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Henry Page
 * @since 1.0.0
 */
@AllArgsConstructor
public enum Domain {
  BIOMEDICAL,
  ENGINEERING,
  INDUSTRIAL,
  SCIENTIFIC;

  public static final String[] STRINGS = {"Biomedical", "Engineering", "Industrial", "Scientific"};
}
