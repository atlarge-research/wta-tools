package com.asml.apa.wta.core.dto;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DstatDataSourceDto class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DstatDto implements SupplierDto{
  private static final long serialVersionUID = 4386177879327585527L;

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
}
