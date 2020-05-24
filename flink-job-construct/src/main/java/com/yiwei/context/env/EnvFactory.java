package com.yiwei.context.env;


import com.yiwei.context.ExecutionContext;
import com.yiwei.sql.config.JobConfig;
import com.yiwei.sql.config.JobRunConfig;
import com.yiwei.sql.config.JobRunType;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author yiwei  2020/4/5
 */
@SuppressWarnings("rawtypes")
public class EnvFactory {

    private static final String EXECUTION_TYPE_KEY = "{EXECUTION_TYPE}";

    private static String defaultEnvSchema =
            "execution:\n" +
                    "  type: " + EXECUTION_TYPE_KEY + "\n" +
                    "  time-characteristic: event-time\n" +
                    "  periodic-watermarks-interval: 200\n" +
                    "  result-mode: table\n" +
                    "  max-table-result-rows: 1000000\n" +
                    "  parallelism: 1\n" +
                    "  max-parallelism: 32\n" +
                    "  min-idle-state-retention: 0\n" +
                    "  max-idle-state-retention: 0\n" +
                    "  restart-strategy:\n" +
                    "    type: fixed-delay\n" +
                    "    attempts: 3\n" +
                    "    delay: 10000\n" +
                    "deployment:\n" +
                    "  response-timeout: 10000\n";

    public static ExecutionContext<?> getExecutionContext(JobConfig jobConfig, List<File> dependencyJars, String sql,
                                                          Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> commandLines) {
        final Environment sessionEnv = new Environment();
        final SessionContext session = new SessionContext(jobConfig.getJobRunConfig().getJobName(), sessionEnv);
        return getExecutionContext(jobConfig.getJobRunConfig(), dependencyJars, flinkConfig, commandLineOptions, commandLines, session);

    }

    private static ExecutionContext<?> getExecutionContext(JobRunConfig jobRunConfig,
                                                           List<File> dependencyJars,
                                                           Configuration flinkConfig,
                                                           Options commandLineOptions,
                                                           List<CustomCommandLine<?>> commandLines,
                                                           SessionContext session) {
        //init env and dependencyJars
        Environment environment = getEnvironment(jobRunConfig);
        //init executionContext
        return createExecutionContext(session, environment, dependencyJars, jobRunConfig, flinkConfig, commandLineOptions, commandLines);
    }

    private static Environment getEnvironment(JobRunConfig jobRunConfig) {
        String envSchema = defaultEnvSchema;

        if (null != jobRunConfig.getJobRunType()) {
            envSchema = envSchema.replace(EXECUTION_TYPE_KEY, jobRunConfig.getJobRunType().name().toLowerCase());
        } else {
            envSchema = envSchema.replace(EXECUTION_TYPE_KEY, JobRunType.STREAMING.name().toLowerCase());
        }


        Environment environment;
        try {
            environment = Environment.parse(envSchema);
        } catch (IOException e) {
            throw new SqlClientException("Could not read default environment", e);
        }

        return environment;
    }

    private static ExecutionContext<?> createExecutionContext(SessionContext session, Environment environment, List<File> dependencies, JobRunConfig jobRunConfig,
                                                              Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> commandLines) {
        ExecutionContext executionContext;
        try {
            executionContext = new ExecutionContext<>(environment, session, dependencies,
                    flinkConfig, commandLineOptions, commandLines, jobRunConfig);
        } catch (Throwable t) {
            // catch everything such that a configuration does not crash the com.ppmon.bigdata.flink.sql.client.executor
            throw new SqlExecutionException("Could not create execution context.", t);
        }

        return executionContext;
    }

}
