package com.yiwei.sql.config;

import lombok.Builder;
import lombok.Data;

/**
 * @author yiwei  2020/4/3
 */
@Data
@Builder
public class JobRunConfig {

    /**
     * job名称
     */
    private String jobName;

    /**
     * checkpoint间隔;单位:ms;默认5min
     */
    private long checkpointInterval;

    /**
     * defaultParallelism;
     */
    private int defaultParallelism;

    /**
     * sourceParallelism;
     * can not config source parallelism
     */
    private int sourceParallelism;

    /**
     * BATCH OR STREAMING
     */
    private JobRunType jobRunType;

    private Boolean isDetached = true;

    private Boolean isRestoreFromCK = true;
}
