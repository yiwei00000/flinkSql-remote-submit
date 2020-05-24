package com.yiwei.local;


import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yiwei.sql.parser.SqlNodeInfo;
import com.yiwei.sql.parser.SqlParserUtil;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 * @author yiwei  2020/4/4
 */
public class SqlJobLocalExecute {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        String sql = null;
        try {
            final File file = new File("/work/workspace/flinkSql-remote-submit/flink-job-construct/sqlfiles/kafkaToConsole-function.sql");
            sql = Files.toString(file, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final List<SqlNodeInfo> sqlNodeInfos = SqlParserUtil.parseSqlContext(sql);
        final List<SqlNodeInfo> insertSqlNodeInfos = SqlParserUtil.getInerstSqlNodeInfos(sqlNodeInfos);
        final List<String> functionSqls = SqlParserUtil.getCreateFunctionSql(sql);
        sqlNodeInfos.forEach(sqlNode -> {
            if (sqlNode.getSqlNode() instanceof SqlCreateTable) {
                createTable(tEnv, sqlNode);
            }
            if (sqlNode.getSqlNode() instanceof SqlCreateView) {
                createView(tEnv, sqlNode);
            }
        });

        functionSqls.forEach(sqlSentence -> {
            final String[] split = sqlSentence.split("\\s+");
            final String funcName = split[2];
            final String funcClass = split[4];
            try {
                createFunction(tEnv, funcName, funcClass);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        });

        insertSqlNodeInfos.forEach(sqlNode -> {
            tEnv.sqlUpdate(sqlNode.getOriginSql());
        });

        // 提交作业
        env.execute("SqlJobLocalExecute");

    }


    private static void createTable(TableEnvironment tEnv, SqlNodeInfo sqlNode) throws SqlExecutionException {

        try {
            tEnv.sqlUpdate(sqlNode.getOriginSql());
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a table from ddl: " + sqlNode.getOriginSql(), e);
        }
    }

    private static void createView(TableEnvironment tEnv, SqlNodeInfo sqlNode) throws SqlExecutionException {

        try {
            String subQuery = ((SqlCreateView) (sqlNode.getSqlNode())).getQuery().toString();
            Table view = tEnv.sqlQuery(subQuery);
            tEnv.registerTable(((SqlCreateView) sqlNode.getSqlNode()).getViewName().toString(), view);
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a view from ddl: " + sqlNode.getOriginSql(), e);
        }
    }

    private static void createFunction(TableEnvironment tEnv, String funcName, String funcClass) throws SqlExecutionException, ClassNotFoundException, IllegalAccessException, InstantiationException {

        System.out.println("createFunction================");
        final Class<?> funcClazz = Class.forName(funcClass);
        final Object o = funcClazz.newInstance();
        tEnv.registerFunction(funcName, (ScalarFunction) o);
        //todo 待添加 tablefunction  aggfunction
    }
}
