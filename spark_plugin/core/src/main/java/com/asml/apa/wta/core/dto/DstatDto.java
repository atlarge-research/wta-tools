package com.asml.apa.wta.core.dto;

import com.asml.apa.wta.core.supplier.DstatSupplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data transfer object for the {@link DstatSupplier}.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DstatDto implements SupplierDto {

  private static final long serialVersionUID = 4386177879327585527L;

  private long totalUsageUsr;

  private long totalUsageSys;

  private long totalUsageIdl;

  private long totalUsageWai;

  private long totalUsageStl;

  private long dskRead;

  private long dskWrite;

  private long netRecv;

  private long netSend;

  private long pagingIn;

  private long pagingOut;

  private long systemInt;

  private long systemCsw;
}
