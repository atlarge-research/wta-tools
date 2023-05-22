package com.asml.apa.wta.core.model.enums;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DomainTest {

  @Test
  void extractNullDomainFails() {
    assertThatThrownBy(() -> Domain.extractAsEnum(null)).isInstanceOf(EnumConstantNotPresentException.class);
  }

  @Test
  void extractEmptyDomainFails() {
    assertThatThrownBy(() -> Domain.extractAsEnum("")).isInstanceOf(EnumConstantNotPresentException.class);
  }

  @Test
  void extractBogusDomainFails() {
    assertThatThrownBy(() -> Domain.extractAsEnum("bogUS")).isInstanceOf(EnumConstantNotPresentException.class);
  }

  @Test
  void extractCapitalDomainSucceeds() {
    assertThat(Domain.extractAsEnum("BIOMEDICAL")).isEqualTo(Domain.BIOMEDICAL);
  }

  @Test
  void extractFirstCapitalDomainSucceeds() {
    assertThat(Domain.extractAsEnum("Scientific")).isEqualTo(Domain.SCIENTIFIC);
  }

  @Test
  void notCaseSensitiveSuceeds() {
    assertThat(Domain.extractAsEnum("ScIEnTifIc")).isEqualTo(Domain.SCIENTIFIC);
  }

  @Test
  void spacesFail() {
    assertThatThrownBy(() -> Domain.extractAsEnum("S CIENTIFIC"))
        .isInstanceOf(EnumConstantNotPresentException.class);
  }
}
