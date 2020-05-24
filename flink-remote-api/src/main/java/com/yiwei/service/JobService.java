package com.yiwei.service;


import com.yiwei.dao.SubmitJobConfig;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
/**
 * @author yiwei  2020/4/11
 */
public interface JobService {

    String submitJob(SubmitJobConfig config) throws Exception;

    boolean cancelJob(String applicationId) throws IOException, YarnException;

    String jobState(String applicationId) throws IOException, YarnException;

    String jobLogAddress(String applicationId) throws IOException, YarnException;

}
