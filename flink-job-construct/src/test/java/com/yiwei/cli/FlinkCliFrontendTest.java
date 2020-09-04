package com.yiwei.cli;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yiwei.context.ExecutionContext;
import com.yiwei.context.env.EnvFactory;
import com.yiwei.sql.SqlEngine;
import com.yiwei.sql.config.JobConfig;
import com.yiwei.sql.config.JobRunConfig;
import com.yiwei.sql.parser.SqlNodeInfo;
import com.yiwei.sql.parser.SqlParserUtil;
import com.yiwei.utils.JarUtils;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yiwei  2020/4/5
 */
public class FlinkCliFrontendTest {

    @Test
    public void cli() throws Exception {
        final String configurationDirectory = FlinkCliFrontend.getConfigurationDirectoryFromEnv();
        final Configuration configuration = new Configuration();
        final List<CustomCommandLine> customCommandLines = FlinkCliFrontend.loadCustomCommandLines(configuration, configurationDirectory);

        final FlinkCliFrontend flinkCliFrontend = new FlinkCliFrontend(configuration, customCommandLines);

        String arg = "-m yarn-cluster -ynm test-command-line -yjm 1024m -ytm 1024m -d";
        String[] argsArr = arg.split(" ");

        final JobGraph jobGraph = getJobGraph(configuration, customCommandLines, flinkCliFrontend);

        flinkCliFrontend.deploy(argsArr, jobGraph);
    }

    private JobGraph getJobGraph(Configuration configuration, List<CustomCommandLine> customCommandLines, FlinkCliFrontend flinkCliFrontend) throws SqlParseException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        String dependencyJarsDir = "/Users/rongjianmin/work/workspace/flinkSql-remote-submit/flink-job-construct/dependencies-1.10.2";
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName("test_deploy")
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
        JobConfig jobConfig = new JobConfig(jobRunConfig, new HashMap<>());
        String sql = null;
        try {
            final File file = new File("/Users/rongjianmin/work/workspace/flinkSql-remote-submit/flink-job-construct/sqlfiles/kafkaToConsole-string.sql");
            sql = Files.toString(file, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<File> dependencyJars = JarUtils.getJars(dependencyJarsDir);
        final Options commandLineOptions = flinkCliFrontend.getCustomCommandLineOptions();
        final ExecutionContext context = EnvFactory.getExecutionContext(jobConfig, dependencyJars, sql, configuration, commandLineOptions, customCommandLines);

        final List<SqlNodeInfo> sqlNodeInfos = SqlParserUtil.parseSqlContext(sql);
        final List<SqlNodeInfo> inerstSqlNodeInfos = SqlParserUtil.getInerstSqlNodeInfos(sqlNodeInfos);
        final List<String> functionSqls = SqlParserUtil.getCreateFunctionSql(sql);
        final SqlEngine sqlEngine = new SqlEngine();
        sqlEngine.registerDDL(sqlNodeInfos, context);
        sqlEngine.registerFunction(functionSqls,context,dependencyJarsDir);

        List<String> insertSqlList = inerstSqlNodeInfos.stream().map(SqlNodeInfo::getOriginSql).collect(Collectors.toList());
        return sqlEngine.jobGraph(context, insertSqlList, jobConfig);
    }

}
