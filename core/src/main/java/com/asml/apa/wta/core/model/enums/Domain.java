package com.asml.apa.wta.core.model.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Domain enum for WTA traces.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Henry Page
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

  /**
   * Extracts the domain value from the given string.
   *
   * @param domain The domain in string format
   * @return The domain in enum format
   * @throws EnumConstantNotPresentException if the given domain is not one of the 4 valid domains
   */
  public static Domain extractAsEnum(String domain) {
    try {
      return Domain.valueOf(domain.toUpperCase());
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new EnumConstantNotPresentException(Domain.class, domain == null ? "null" : domain.toUpperCase());
    }
  }
}
