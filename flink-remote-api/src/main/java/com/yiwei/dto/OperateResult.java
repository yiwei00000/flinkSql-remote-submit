package com.yiwei.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author yiwei  2020/4/11
 */
@Getter
@Setter
@ToString
public class OperateResult extends BasicResult {

    private String applicationId;

    private String logAddress;


}
