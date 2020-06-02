# 远程提交flink sql 到  yarn集群
模块介绍:  
1、flink-sql-mix：定义source、sink及udf模块  
2、flink-remote-api：rest提交模块  
3、flink-job-construct：构造jobgrap及提交到yarn模块  

本地提交：  
入口类：com.yiwei.local.SqlJobLocalExecute  
注意事项：
1、sql文件路径改成自己的路径  
2、依赖jar文件夹改为自己的路径  
3、resources中的配置文件替换成你的集群的中的配置文件  
  
rest远程提交：  
1、启动SqlApplication程序  
2、访问localhost:port/swagger-ui.html  
提交接口：http://localhost:port/job/submit  
提交方法:Post  
参数样例：  
{  
 "dependencyJarsDir": "./dependencies",  
 "sql": "/qsc/flink-sql-api/sqlsumbit/kafkaToConsole-function.sql"  
}

若有问题请添加微信：yiwei0991

