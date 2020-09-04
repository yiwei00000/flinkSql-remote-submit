package com.yiwei.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yiwei  2020/4/4
 */
public class SqlParserUtil {

    private static final SqlParser.Config PARSER_CONFIG;
    private static final FrameworkConfig FRAMEWORK_CONFIG;
    private static final SchemaPlus ROOT_SCHEMA;

    static {
        PARSER_CONFIG = SqlParser.configBuilder().setParserFactory(FlinkSqlParserImpl.FACTORY).setQuoting(Quoting.BACK_TICK).setQuotedCasing(Casing.UNCHANGED).setUnquotedCasing(Casing.UNCHANGED).setConformance(SqlConformanceEnum.DEFAULT).setIdentifierMaxLength(256).setLex(Lex.JAVA).build();
        ROOT_SCHEMA = Frameworks.createRootSchema(true);
        FRAMEWORK_CONFIG = Frameworks.newConfigBuilder().defaultSchema(ROOT_SCHEMA).parserConfig(PARSER_CONFIG).typeSystem(RelDataTypeSystem.DEFAULT).build();
    }

    /**
     * parseSqlContext 当前不支持 create function 语法; 可以单独为create function 语法执行逻辑分支进行分析并返回;
     * 支持create table;create view;insert into
     */
    public static List<SqlNodeInfo> parseSqlContext(String sql) {
        FlinkPlannerImpl planner = new FlinkPlannerImpl(FRAMEWORK_CONFIG, null, null, null);
        List<SqlInfo> sqlInfos = SqlLists.getSQLList(sql);

        List<SqlNodeInfo> sqlNodeInfoList = new ArrayList();
        for (SqlInfo sqlInfo : sqlInfos) {
            if (StringUtils.isBlank(sqlInfo.getSqlContent()) || sqlInfo.getSqlContent().contains("create function")) {
                continue;
            }

            SqlNode sqlNode = planner.parser().parse(sqlInfo.getSqlContent());
            SqlNodeInfo sqlNodeInfo = new SqlNodeInfo(sqlNode, sqlInfo.getSqlContent());
            sqlNodeInfoList.add(sqlNodeInfo);
        }

        return sqlNodeInfoList;
    }

    /**
     * 获得create function 语句，中间只能有一个空格
     */
    public static List<String> getCreateFunctionSql(String sql) {
        List<SqlInfo> sqlInfos = SqlLists.getSQLList(sql);
        List<String> functionSqls = new ArrayList();
        for (SqlInfo sqlInfo : sqlInfos) {
            if (StringUtils.isNotBlank(sqlInfo.getSqlContent()) && sqlInfo.getSqlContent().contains("create function")) {
                functionSqls.add(sqlInfo.getSqlContent());
            }
        }

        return functionSqls;
    }

    public static List<SqlNodeInfo> getInerstSqlNodeInfos(List<SqlNodeInfo> sqlNodeList) {
        return sqlNodeList
                .stream()
                .filter(node -> SqlKind.INSERT.lowerName.equalsIgnoreCase(node.getSqlNode().getKind().lowerName))
                .collect(Collectors.toList());
    }
}
