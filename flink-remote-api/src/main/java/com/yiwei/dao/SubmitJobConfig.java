package com.yiwei.dao;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * @author yiwei  2020/4/11
 */
@Getter
@Setter
@ToString
public class SubmitJobConfig {

    private String jobName = "Flink-Job" + new Date().getTime();
    @NonNull
    private String sql;
    private String runMode = "yarn-cluster";
    private String jmMemSize = "1204m";
    private String tmMemSize = "1204m";
    private int parallelism = 1;
    private long ckInterval = 60000L;
    private String dependencyJarsDir = "./dependencies";

}
