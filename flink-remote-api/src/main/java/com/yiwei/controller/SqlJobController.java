package com.yiwei.controller;

import com.alibaba.fastjson.JSON;
import com.yiwei.dao.SubmitJobConfig;
import com.yiwei.dto.OperateResult;
import com.yiwei.service.JobService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * @author yiwei  2020/4/11
 */
@SuppressWarnings("SpringMVCViewInspection")
@RestController
@RequestMapping("/job")
public class SqlJobController {

    @Autowired
    JobService jobService;

    @PostMapping("/submit")
    public String submitJob(@RequestBody SubmitJobConfig config) {
        OperateResult opRs = new OperateResult();
        try {
            final String applicationId = jobService.submitJob(config);
            opRs.setCode(0);
            opRs.setApplicationId(applicationId);
            opRs.setMessage("The Job " + config.getJobName() + ":" + applicationId + " has been submitted!!!");
        } catch (Exception e) {
            e.printStackTrace();
            opRs.setCode(1);
            opRs.setMessage("The Job " + config.getJobName() + "  submit field!!! , error msg : " + e.getMessage());
        }
        return JSON.toJSONString(opRs);
    }

    @GetMapping("/cancel")
    public String cancelJob(@RequestParam String applicationIdStr) {
        OperateResult opRs = new OperateResult();
        opRs.setCode(1);
        opRs.setApplicationId(applicationIdStr);
        if (StringUtils.isNotBlank(applicationIdStr)) {
            final String[] appArr = applicationIdStr.split("_");
            if (appArr.length == 3) {
                try {
                    final boolean canceled = jobService.cancelJob(applicationIdStr);
                    if (canceled) {
                        opRs.setCode(0);
                        opRs.setMessage("Job " + applicationIdStr + " has been canceled!");
                    } else {
                        opRs.setCode(1);
                        opRs.setMessage("Job " + applicationIdStr + " cancel field!");
                    }

                } catch (NumberFormatException | YarnException | IOException e) {
                    e.printStackTrace();
                    opRs.setMessage("error :" + e.getMessage());
                }
            } else {
                opRs.setMessage("error : Incorrect format of applicationId . Such as application_1587990549973_0138,but your input is " + applicationIdStr + ".");
            }
        } else {
            opRs.setMessage("error : ApplicationId should not be null!");
        }

        return JSON.toJSONString(opRs);
    }

    @GetMapping("/status")
    public String jobStatus(@RequestParam String applicationIdStr) {

        OperateResult opRs = new OperateResult();
        opRs.setCode(1);
        opRs.setApplicationId(applicationIdStr);
        if (StringUtils.isNotBlank(applicationIdStr)) {
            final String[] appArr = applicationIdStr.split("_");
            if (appArr.length == 3) {
                try {
                    final String sate = jobService.jobState(applicationIdStr);
                    opRs.setCode(0);
                    opRs.setMessage(sate);
                } catch (NumberFormatException | YarnException | IOException e) {
                    e.printStackTrace();
                    opRs.setMessage("error :" + e.getMessage());
                }
            } else {
                opRs.setMessage("error : Incorrect format of applicationId . Such as application_1587990549973_0138,but your input is " + applicationIdStr + ".");
            }
        } else {
            opRs.setMessage("error : ApplicationId should not be null!");
        }

        return JSON.toJSONString(opRs);

    }

    @GetMapping("/logAddress")
    public String jobLogAddress(@RequestParam String applicationIdStr) {

        OperateResult opRs = new OperateResult();
        opRs.setCode(1);
        opRs.setApplicationId(applicationIdStr);
        if (StringUtils.isNotBlank(applicationIdStr)) {
            final String[] appArr = applicationIdStr.split("_");
            if (appArr.length == 3) {
                try {
                    final String logAddress = jobService.jobLogAddress(applicationIdStr);
                    opRs.setCode(0);
                    opRs.setLogAddress(logAddress);
                } catch (NumberFormatException | YarnException | IOException e) {
                    e.printStackTrace();
                    opRs.setMessage("error :" + e.getMessage());
                }
            } else {
                opRs.setMessage("error : Incorrect format of applicationId . Such as application_1587990549973_0138,but your input is " + applicationIdStr + ".");
            }
        } else {
            opRs.setMessage("error : ApplicationId should not be null!");
        }

        return JSON.toJSONString(opRs);

    }

    @PostMapping("/sqlValidate")
    public String sqlValidate(@RequestBody SubmitJobConfig config) {

        OperateResult opRs = new OperateResult();
        try {
            jobService.sqlValidate(config);
            opRs.setCode(0);
            opRs.setMessage("Sql has been validated!!!");
        } catch (Exception e) {
            e.printStackTrace();
            opRs.setCode(1);
            opRs.setMessage("Sql validated field!!! , error msg : " + e.getMessage());
        }
        return JSON.toJSONString(opRs);

    }
}
