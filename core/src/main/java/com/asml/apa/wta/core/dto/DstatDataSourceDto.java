package com.asml.apa.wta.core.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * IostatDataSourceDto class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DstatDataSourceDto {
    private int totalUsageUsr;
    private int totalUsageSys;
    private int totalUsageIdl;
    private int totalUsageWai;
    private int totalUsageStl;
    private int dskRead;
    private int dskWrite;
    private int netRecv;
    private int netSend;
    private int pagingIn;
    private int pagingOut;
    private int systemInt;
    private int systemCsw;

    private String executorId;

    @Builder.Default
    private long timestamp = Instant.now().toEpochMilli();
}

