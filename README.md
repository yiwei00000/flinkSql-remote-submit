# 远程提交flink sql 到  yarn集群
目前支持flink-1.10.2版本（改造中，还不可用） 

模块介绍:  
1、flink-sql-mix：定义source、sink、udf/udaf/udtf模块,支持该模块的热部署（及每次提交都会重新加载该模块的jar包）  
2、flink-remote-api：rest提交模块，支持提交作业、取消作业、查看作业状态、校验sql等  
3、flink-job-construct：构造jobgraph及提交到yarn模块  

本地运行：  
入口类：com.yiwei.local.SqlJobLocalExecute  
注意事项：  
1、sql文件路径改成自己的路径  
2、依赖jar文件夹改为自己的路径
     
  
rest远程提交：  
1、启动SqlApplication程序  
2、访问ip:port/swagger-ui.html   
提交接口：http://ip:port/job/submit  
提交方法:Post  
参数样例：  
{  
 "dependencyJarsDir": "./dependencies",  
 "sql": "${path}/kafkaToConsole-function.sql"  
}  
${path}根据文件位置替换

rest提交注意事项：  
1、程序部署服务器要配置FLINK_CONF_DIR,只用于读取配置文件   
2、flink*.jar要放到dependencies下,不再去flink/lib下加载 ;
   特别注意：由于flink-dist*.jar在源码AbstractYarnClusterDescriptor类中做了过滤，所以该
   包名做要改一下。比如改成  flink-all-dist*.jar
3、http://ip:port/job/submit 接口嵌入前端界面可以直接写sql，也可以指定部署服务器上的sql路径  
4、dependencyJarsDir依赖jar包的路径可以根据自己部署路径任意变动，要做出适当修改  
5、resources中的配置文件替换成你的集群的中的配置文件


若有问题请添加微信：yiwei0991

