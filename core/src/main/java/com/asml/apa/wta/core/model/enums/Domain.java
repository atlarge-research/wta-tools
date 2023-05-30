package com.asml.apa.wta.core.model.enums;

import com.google.gson.annotations.SerializedName;

/**
 * Domain enum for WTA traces.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Henry Page
 * @since 1.0.0
 */
public enum Domain {
  @SerializedName("Biomedical")
  BIOMEDICAL,

  @SerializedName("Engineering")
  ENGINEERING,

  @SerializedName("Industrial")
  INDUSTRIAL,

  @SerializedName("Scientific")
  SCIENTIFIC;

  public static final String[] STRINGS = {"Biomedical", "Engineering", "Industrial", "Scientific"};
}
