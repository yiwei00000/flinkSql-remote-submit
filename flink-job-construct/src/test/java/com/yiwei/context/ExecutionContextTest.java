package com.yiwei.context;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yiwei.cli.FlinkCliFrontend;
import com.yiwei.context.env.EnvFactory;
import com.yiwei.sql.config.JobConfig;
import com.yiwei.sql.config.JobRunConfig;
import com.yiwei.utils.JarUtils;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * @author yiwei  2020/4/5
 */
@SuppressWarnings("UnstableApiUsage")
public class ExecutionContextTest {

    @Test
    public void context() throws Exception {
        String dependencyJarsDir = "./dependencies";
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName("test_deploy")
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
        JobConfig jobConfig = new JobConfig(jobRunConfig, new HashMap<>());
        String sql = null;
        try {
            final File file = new File("/work/workspace/flinkSql-remote-submit/flink-job-construct/sqlfiles/kafkaToConsole-function.sql");
            sql = Files.toString(file, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<File> dependencyJars = JarUtils.getJars(dependencyJarsDir);
        final Configuration flinkCfg = new Configuration();
        flinkCfg.setString("m", "yarn-cluster");
        final FlinkCliFrontend flinkCliFrontend = new FlinkCliFrontend(flinkCfg);
        final Options commandLineOptions = flinkCliFrontend.getCustomCommandLineOptions();
        final Option option = new Option("m", "", false, "yarn-cluster");
        commandLineOptions.addOption(option);
        final List<CustomCommandLine<?>> customCommandLines = FlinkCliFrontend.loadCustomCommandLines(new Configuration());
        final ExecutionContext<?> context = EnvFactory.getExecutionContext(jobConfig, dependencyJars, sql, flinkCfg, commandLineOptions, customCommandLines);
        final ClusterDescriptor<?> clusterDescriptor = context.createClusterDescriptor();
        System.out.println(clusterDescriptor);
    }
}
