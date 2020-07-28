package com.yiwei.sql;


import com.yiwei.context.ExecutionContext;
import com.yiwei.sql.config.JobConfig;
import com.yiwei.sql.parser.SqlNodeInfo;
import com.yiwei.utils.ClassLoaderUtil;
import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.net.MalformedURLException;
import java.util.List;

/**
 * @author yiwei  2020/4/4
 */
@SuppressWarnings("rawtypes")
public class SqlEngine {

    public <C> JobGraph jobGraph(ExecutionContext<C> context, List<String> statements, JobConfig jobConfig) {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();

        ExecutionConfig.GlobalJobParameters globalJobParameters = envInst.getExecutionConfig().getGlobalJobParameters();
        if (globalJobParameters == null) {
            globalJobParameters = new Configuration();
        }

        Configuration configuration = (Configuration) globalJobParameters;
        jobConfig.getJobParameter().forEach(configuration::setString);
        envInst.getExecutionConfig().setGlobalJobParameters(configuration);

        statements.forEach(statement -> applyUpdate(context, envInst.getTableEnvironment(), statement));

        // create job graph with dependencies
        final String jobName = jobConfig.getJobRunConfig().getJobName();
        final JobGraph jobGraph;
        try {
            // createJobGraph requires an optimization step that might reference UDFs during code compilation
            jobGraph = context.wrapClassLoader(() -> envInst.createJobGraph(jobName));
        } catch (Throwable t) {
            // catch everything such that the statement does not crash the com.ppmon.bigdata.flink.sql.client.executor
            throw new SqlExecutionException("Invalid SQL statement.", t);
        }
        return jobGraph;

    }

    /**
     * Applies the given update statement to the given table environment with query configuration.
     */
    private <C> void applyUpdate(ExecutionContext<C> context, TableEnvironment tableEnv, String updateStatement) {
        // parse and validate statement
        try {
            // update statement requires an optimization step that might reference UDFs during code compilation
            context.wrapClassLoader(() -> {
                tableEnv.sqlUpdate(updateStatement);
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
            // catch everything such that the statement does not crash the com.ppmon.bigdata.flink.sql.client.executor
            throw new SqlExecutionException("Invalid SQL update statement.", e);
        }
    }

    public void registerDDL(List<SqlNodeInfo> sqlNodeInfoList, ExecutionContext<?> context) {
        //TODO 按照 function -> createTable -> createView 进行排序

        sqlNodeInfoList.forEach(sqlNode -> {
            if (sqlNode.getSqlNode() instanceof SqlCreateTable) {
                this.createTable(context, sqlNode);
            }
            if (sqlNode.getSqlNode() instanceof SqlCreateView) {
                this.createView(context, sqlNode);
            }
        });
    }

    public void registerFunction(List<String> funcSentences, ExecutionContext<?> context) throws IllegalAccessException, InstantiationException, ClassNotFoundException {

        for (String funcSentence : funcSentences) {
            final String[] split = funcSentence.replaceAll("\\n", "").split("\\s+");
            final String funcName = split[2];
            final String funcClass = split[4].replace("'","");
            this.createFunction(context,funcName, funcClass);
        }
    }

    private void createTable(ExecutionContext<?> context, SqlNodeInfo sqlNode) throws SqlExecutionException {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            tEnv.sqlUpdate(sqlNode.getOriginSql());
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a table from ddl: " + sqlNode.getOriginSql(), e);
        }
    }

    private void createView(ExecutionContext<?> context, SqlNodeInfo sqlNode) throws SqlExecutionException {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            String subQuery = ((SqlCreateView) (sqlNode.getSqlNode())).getQuery().toString();
            Table view = tEnv.sqlQuery(subQuery);
            tEnv.registerTable(((SqlCreateView) sqlNode.getSqlNode()).getViewName().toString(), view);
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a view from ddl: " + sqlNode.getOriginSql(), e);
        }
    }

    private void createFunction(ExecutionContext<?> context, String funcName, String funcClass) throws SqlExecutionException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        StreamTableEnvironment tEnv = (StreamTableEnvironment)envInst.getTableEnvironment();

        //每次创建func时，重新加载jar包
        final ClassLoaderUtil classLoaderUtil = new ClassLoaderUtil();
        String jarName = "flink-sql-utils-1.0-SNAPSHOT.jar";
        String jarPath = "";
        final String userDir = System.getProperties().getProperty("user.dir");
        jarPath = userDir + "/../dependencies";

        classLoaderUtil.unloadJarFile(jarName, jarPath);
        try {
            classLoaderUtil.loadJar(jarName, jarPath);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        final Class<?> funcClazz = classLoaderUtil.loadClass(jarName, funcClass);
        final Object o = funcClazz.newInstance();
        if(o instanceof ScalarFunction){
            tEnv.registerFunction(funcName, (ScalarFunction) o);
        }else if(o instanceof AggregateFunction){
            tEnv.registerFunction(funcName,(AggregateFunction) o);
        }else if(o instanceof TableFunction){
            tEnv.registerFunction(funcName,(TableFunction) o);
        }
    }
}
