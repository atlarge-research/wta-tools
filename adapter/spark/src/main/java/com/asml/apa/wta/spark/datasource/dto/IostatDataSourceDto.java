package com.asml.apa.wta.spark.datasource.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Builder;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IostatDataSourceDto {
    private double tps;
    private double KBReadPerSec;
    private double KBWrtnPerSec;
    private double KBDscdPerSec;
    private double KBRead;
    private double KBWrtn;
    private double KBDscd;
    private String executorId;
}
