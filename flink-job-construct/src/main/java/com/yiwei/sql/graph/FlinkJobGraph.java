package com.yiwei.sql.graph;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yiwei.cli.FlinkCliFrontend;
import com.yiwei.context.ExecutionContext;
import com.yiwei.context.env.EnvFactory;
import com.yiwei.sql.SqlEngine;
import com.yiwei.sql.config.JobConfig;
import com.yiwei.sql.config.JobRunConfig;
import com.yiwei.sql.parser.SqlNodeInfo;
import com.yiwei.sql.parser.SqlParserUtil;
import com.yiwei.utils.JarUtils;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yiwei  2020/4/5
 */
@SuppressWarnings("UnstableApiUsage")
public class FlinkJobGraph {


    public static JobGraph jobGraph(Configuration configuration,
                                    List<CustomCommandLine<?>> customCommandLines,
                                    FlinkCliFrontend flinkCliFrontend,
                                    JobRunConfig jobRunConfig,
                                    String sqlOrFile,
                                    String dependencyJarsDir) throws IllegalAccessException, ClassNotFoundException, InstantiationException {

        final File sqlFile = new File(sqlOrFile);
        String sql = null;
        if (sqlFile.isFile()) {

            try {
                sql = Files.toString(sqlFile, Charsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            sql = sqlOrFile;
        }

        JobConfig jobConfig = new JobConfig(jobRunConfig, new HashMap<>());
        List<File> dependencyJars = JarUtils.getJars(dependencyJarsDir);
        final Options commandLineOptions = flinkCliFrontend.getCustomCommandLineOptions();
        final ExecutionContext<?> context = EnvFactory.getExecutionContext(jobConfig, dependencyJars, sql, configuration, commandLineOptions, customCommandLines);

        final List<SqlNodeInfo> sqlNodeInfos = SqlParserUtil.parseSqlContext(sql);
        final List<SqlNodeInfo> insertSqlNodeInfos = SqlParserUtil.getInerstSqlNodeInfos(sqlNodeInfos);
        final List<String> functionSqls = SqlParserUtil.getCreateFunctionSql(sql);
        final SqlEngine sqlEngine = new SqlEngine();
        sqlEngine.registerDDL(sqlNodeInfos, context);
        sqlEngine.registerFunction(functionSqls, context,dependencyJarsDir);

        List<String> insertSqlList = insertSqlNodeInfos.stream().map(SqlNodeInfo::getOriginSql).collect(Collectors.toList());
        return sqlEngine.jobGraph(context, insertSqlList, jobConfig);

    }

}
