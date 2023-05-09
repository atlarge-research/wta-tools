package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.Config.RuntimeConfig;
import com.asml.apa.wta.core.Utils.WTAUtils;
import org.junit.jupiter.api.Test;

public class ConfigReaderTest {
  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = WTAUtils.readConfig("src/test/resources/testConfig.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getSchemaVersion()).isEqualTo("1.0.0");
  }

  @Test
  void readsConfigFileWhereTheDescriptionIsNotThere() {
    RuntimeConfig cr = WTAUtils.readConfig("src/test/resources/testConfigNoDesc.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("");
    assertThat(cr.getSchemaVersion()).isEqualTo("1.0.0");
  }

  @Test
  void readsConfigFileWhereTheAuthorIsNotThere() {
    assertThatThrownBy(() -> {
          WTAUtils.readConfig("src/test/resources/testConfigInvalid.json");
        })
        .isInstanceOf(IllegalArgumentException.class);
  }
}
