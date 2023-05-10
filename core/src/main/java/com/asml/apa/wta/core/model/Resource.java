package com.asml.apa.wta.core.model;

import lombok.Builder;
import lombok.Data;
import lombok.Setter;

/**
 * Resource class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Resource implements BaseTraceObject{

    private static final long serialVersionUID = 3002249398331752973L;

    @Setter
    private static String schemaVersion;

    private final long id;

    private final String type;

    private final double numResources;

    private final String procModel;

    private final long memory;

    private final long diskSpace;

    private final long networkSpeed;

    private final String os;

    private final String details;
}
