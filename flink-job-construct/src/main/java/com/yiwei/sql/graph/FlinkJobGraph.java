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
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.METADATA_FILE_NAME;

/**
 * @author yiwei  2020/4/5
 */
@SuppressWarnings("UnstableApiUsage")
public class FlinkJobGraph {

    private final static Logger logger = LoggerFactory.getLogger(FlinkJobGraph.class);

    private final static String CHECKPOINT_PATH = "hdfs:///flink/checkpoint/";


    public static JobGraph jobGraph(Configuration configuration,
                                    List<CustomCommandLine> customCommandLines,
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
        final ExecutionContext context = EnvFactory.getExecutionContext(jobConfig, dependencyJars, sql, configuration, commandLineOptions, customCommandLines);

        final List<SqlNodeInfo> sqlNodeInfos = SqlParserUtil.parseSqlContext(sql);
        final List<SqlNodeInfo> insertSqlNodeInfos = SqlParserUtil.getInerstSqlNodeInfos(sqlNodeInfos);
        final List<String> functionSqls = SqlParserUtil.getCreateFunctionSql(sql);
        final SqlEngine sqlEngine = new SqlEngine();
        sqlEngine.registerDDL(sqlNodeInfos, context);
        sqlEngine.registerFunction(functionSqls, context,dependencyJarsDir);

        List<String> insertSqlList = insertSqlNodeInfos.stream().map(SqlNodeInfo::getOriginSql).collect(Collectors.toList());

        final JobGraph jobGraph = sqlEngine.jobGraph(context, insertSqlList, jobConfig);

        // 是否从ck状态恢复
        if(jobRunConfig.getIsRestoreFromCK()){
            setSavepoint(jobGraph, jobRunConfig);
        }

        return jobGraph;

    }

    private static void setSavepoint(JobGraph jobGraph, JobRunConfig jobRunConfig) {
        Path appCheckpointPath = new Path(CHECKPOINT_PATH, jobRunConfig.getJobName());
        // 设置作业的savepoint
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.none());
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            if (!fileSystem.exists(appCheckpointPath)) {
                // 没有app对应的checkpoint则不处理
                logger.info("没有找到checkpoint文件");
                return;
            }
            List<FileStatus> appCheckDirFiles = Stream.of(fileSystem.listStatus(new Path(CHECKPOINT_PATH, jobRunConfig.getJobName())))
                    .flatMap(file -> {
                        FileStatus[] fileStatuses = null;
                        try {
                            fileStatuses = fileSystem.listStatus(file.getPath());
                        } catch (IOException e) {
                            logger.error("查看文件列表失败", e);
                        }
                        return Stream.of(fileStatuses);
                    })
                    .filter(file -> file.getPath().getName().startsWith(AbstractFsCheckpointStorage.CHECKPOINT_DIR_PREFIX))
                    .sorted((x, y) -> Long.compare(y.getModificationTime(), x.getModificationTime()))
                    .collect(Collectors.toList());
            for (FileStatus fileStatus : appCheckDirFiles) {
                Path metadataFile = new Path(fileStatus.getPath().toString(), METADATA_FILE_NAME);
                if (fileSystem.exists(metadataFile)) {
                    //allowNonRestoredState （可选）：布尔值，指定如果保存点包含无法映射回作业的状态，是否应拒绝作业提交。 default is false
                    logger.info("Find Savepoint {}", metadataFile);
                    jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(fileStatus.getPath().toString(), true));
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("获取filsystem失败", e);
        }
    }

}
