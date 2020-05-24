package com.yiwei.utils;



import com.yiwei.dto.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

import static java.lang.Integer.parseInt;

/**
 * @author yiwei  2020/4/11
 */
public class YarnClientUtil {

    public static YarnClient init() {
        YarnClient client = new YarnClientImpl();
        client.init(new Configuration());
        client.start();
        return client;
    }


    public static Tuple2<ApplicationReport, YarnClient> getApplicationReport(String applicationIdStr) throws IOException, YarnException {
        final String[] appArr = applicationIdStr.split("_");
        long clusterTimestamp = Long.parseLong(appArr[1]);
        int id = parseInt(appArr[2]);
        final ApplicationId applicationId = ApplicationId.newInstance(clusterTimestamp, id);
        final YarnClient client = YarnClientUtil.init();
        final ApplicationReport applicationReport = client.getApplicationReport(applicationId);
        return new Tuple2<>(applicationReport, client);
    }

}
