package com.yiwei.service.impl;


import com.yiwei.cli.FlinkCliFrontend;
import com.yiwei.dao.SubmitJobConfig;
import com.yiwei.dto.Tuple2;
import com.yiwei.service.JobService;
import com.yiwei.sql.config.JobRunConfig;
import com.yiwei.sql.config.JobRunType;
import com.yiwei.sql.graph.FlinkJobGraph;
import com.yiwei.sql.parser.SqlParserUtil;
import com.yiwei.utils.YarnClientUtil;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * @author yiwei  2020/4/11
 */
@SuppressWarnings("DuplicatedCode")
@Service
public class JobServiceImpl implements JobService {
    @Override
    public String submitJob(SubmitJobConfig config) throws Exception {
        final JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(config.getJobName())
                .jobRunType(JobRunType.STREAMING)
                .sourceParallelism(config.getParallelism())
                .defaultParallelism(config.getParallelism())
                .isDetached(true)
                .checkpointInterval(config.getCkInterval())
                .build();
        final String dependencyJarsDir = config.getDependencyJarsDir();
        final String configurationDirectory = FlinkCliFrontend.getConfigurationDirectoryFromEnv();
        final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        final List<CustomCommandLine> customCommandLines = FlinkCliFrontend.loadCustomCommandLines(configuration, configurationDirectory);
        final FlinkCliFrontend flinkCliFrontend = new FlinkCliFrontend(configuration, customCommandLines);

        final JobGraph jobGraph = FlinkJobGraph.jobGraph(configuration, customCommandLines, flinkCliFrontend, jobRunConfig, config.getSql(), dependencyJarsDir);

        final String[] args = generateArgs(config);

        final Object applicationId = flinkCliFrontend.deploy(args, jobGraph);

        return applicationId == null ? "" : applicationId.toString();
    }

    private String[] generateArgs(SubmitJobConfig config) {
        StringBuilder sb = new StringBuilder();
        sb.append("-m").append(" ").append(config.getRunMode()).append(" ");
        sb.append("-yjm").append(" ").append(config.getJmMemSize()).append(" ");
        sb.append("-ytm").append(" ").append(config.getTmMemSize()).append(" ");
        sb.append("-ynm").append(" ").append(config.getJobName()).append(" ");
        sb.append("-d");
        return sb.toString().trim().split("\\s+");
    }

    @Override
    public boolean cancelJob(String applicationIdStr) throws IOException, YarnException {

        final String[] appArr = applicationIdStr.split("_");
        long clusterTimestamp = Long.parseLong(appArr[1]);
        int id = parseInt(appArr[2]);
        final ApplicationId applicationId = ApplicationId.newInstance(clusterTimestamp, id);
        final YarnClient client = YarnClientUtil.init();
        client.killApplication(applicationId);
        client.close();

        return true;
    }

    @Override
    public String jobState(String applicationIdStr) throws IOException, YarnException {

        final Tuple2<ApplicationReport, YarnClient> tuple2 = YarnClientUtil.getApplicationReport(applicationIdStr);
        final YarnApplicationState state = tuple2._1.getYarnApplicationState();
        tuple2._2.close();

        return state.toString();
    }

    @Override
    public String jobLogAddress(String applicationIdStr) throws IOException, YarnException {

        final Tuple2<ApplicationReport, YarnClient> tuple2 = YarnClientUtil.getApplicationReport(applicationIdStr);
        final String trackingUrl = tuple2._1.getOriginalTrackingUrl();
        tuple2._2.close();

        return trackingUrl;
    }

    @Override
    public void sqlValidate(SubmitJobConfig config) {
        SqlParserUtil.parseSqlContext(config.getSql());
    }
}
