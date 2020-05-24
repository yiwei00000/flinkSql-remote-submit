package com.yiwei.util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yiwei.sql.parser.SqlNodeInfo;
import com.yiwei.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author yiwei  2020/4/3
 */
public class SqlParserUtilTest {

    @Test
    public void sqlParse() throws SqlParseException {
        String sql = null;
        try {
            final File file = new File("/work/workspace/flinkSql-remote-submit/flink-job-construct/sqlfiles/kafkaToConsole-function.sql");
            sql = Files.toString(file, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final List<SqlNodeInfo> sqlNodeInfos = SqlParserUtil.parseSqlContext(sql);
        final List<String> createFunctionSql = SqlParserUtil.getCreateFunctionSql(sql);
        System.out.println(createFunctionSql.get(0));
        for (SqlNodeInfo sqlNodeInfo : sqlNodeInfos) {
            System.out.println(sqlNodeInfo.getOriginSql());
        }
    }
}
