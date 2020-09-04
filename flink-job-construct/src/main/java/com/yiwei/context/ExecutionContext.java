package com.yiwei.context;


import com.yiwei.sql.config.JobRunConfig;
import com.yiwei.utils.JarUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.deployment.executors.ExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.TemporaryClassLoaderContext;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ExecutionEntry;
import org.apache.flink.table.client.config.entries.SinkTableEntry;
import org.apache.flink.table.client.config.entries.SourceSinkTableEntry;
import org.apache.flink.table.client.config.entries.SourceTableEntry;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.functions.FunctionService;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.delegation.ExecutorBase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.METADATA_FILE_NAME;

/**
 * @author yiwei  2020/4/5
 */
public class ExecutionContext {

    private final static Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);
    private final static String CHECKPOINT_PATH = "hdfs:///flink/checkpoint/";

    private final Environment mergedEnv;
    private final List<File> dependencies;
    private final ClassLoader classLoader;
    private final Configuration flinkConfig;
    private final Map<String, TableSource> tableSources;
    private final Map<String, TableSink<?>> tableSinks;
    private final Map<String, Catalog> catalogs;
    private final Map<String, UserDefinedFunction> functions;

    private final ExecutionContext.EnvironmentInstance environmentInstance;

    /**
     * 默认5分钟间隔 checkpoint
     */
    private final long DEFAULT_CHECKPOINT_INTERVAL = 5 * 60_000L;
    private final boolean DEFAULT_ENABLE_CHECKPOINT = true;
    private final String DEFAULT_JOB_RUN_NAME = "Flink-job";

    private final JobRunConfig DEFAULT_JOB_RUN_CONFIG = JobRunConfig.builder()
            .jobName(DEFAULT_JOB_RUN_NAME)
            .checkpointInterval(DEFAULT_CHECKPOINT_INTERVAL)
            .build();

    public ExecutionContext(Environment defaultEnvironment, List<File> dependencies, Configuration flinkConfig, JobRunConfig jobRunConfig) throws MalformedURLException {

        this.mergedEnv = defaultEnvironment;
        this.dependencies = dependencies;
        this.flinkConfig = flinkConfig;

        List<URL> dependencyUrls = JarUtils.toURLs(dependencies);

        // create class loader 并设定当前线程classloader
        classLoader = FlinkUserCodeClassLoaders.parentFirst(
                dependencyUrls.toArray(new URL[dependencies.size()]),
                this.getClass().getClassLoader(), null);
        Thread.currentThread().setContextClassLoader(classLoader);

        // create catalogs
        catalogs = new LinkedHashMap<>();
        mergedEnv.getCatalogs().forEach((name, entry) ->
                catalogs.put(name, createCatalog(name, entry.asMap(), classLoader))
        );

        // create table sources & sinks.
        tableSources = new LinkedHashMap<>();
        tableSinks = new LinkedHashMap<>();
        mergedEnv.getTables().forEach((name, entry) -> {
            if (entry instanceof SourceTableEntry || entry instanceof SourceSinkTableEntry) {
                tableSources.put(name, createTableSource(mergedEnv.getExecution(), entry.asMap(), classLoader));
            }
            if (entry instanceof SinkTableEntry || entry instanceof SourceSinkTableEntry) {
                tableSinks.put(name, createTableSink(mergedEnv.getExecution(), entry.asMap(), classLoader));
            }
        });

        // create user-defined functions
        functions = new LinkedHashMap<>();
        mergedEnv.getFunctions().forEach((name, entry) -> {
            final UserDefinedFunction function = FunctionService.createFunction(entry.getDescriptor(), classLoader, false);
            functions.put(name, function);
        });


        // always share environment instance
        if (null == jobRunConfig) {
            environmentInstance = new ExecutionContext.EnvironmentInstance(DEFAULT_ENABLE_CHECKPOINT, DEFAULT_JOB_RUN_CONFIG);
        } else {
            environmentInstance = new ExecutionContext.EnvironmentInstance(DEFAULT_ENABLE_CHECKPOINT, jobRunConfig);
        }
    }

    private Catalog createCatalog(String name, Map<String, String> catalogProperties, ClassLoader classLoader) {
        final CatalogFactory factory =
                TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
        return factory.createCatalog(name, catalogProperties);
    }

    private static TableSource createTableSource(ExecutionEntry execution, Map<String, String> sourceProperties, ClassLoader classLoader) {
        if (execution.isStreamingPlanner()) {
            final StreamTableSourceFactory<?> factory = (StreamTableSourceFactory<?>)
                    TableFactoryService.find(StreamTableSourceFactory.class, sourceProperties, classLoader);
            return factory.createStreamTableSource(sourceProperties);
        } else if (execution.isBatchPlanner()) {
            final BatchTableSourceFactory<?> factory = (BatchTableSourceFactory<?>)
                    TableFactoryService.find(BatchTableSourceFactory.class, sourceProperties, classLoader);
            return factory.createBatchTableSource(sourceProperties);
        }
        throw new SqlExecutionException("Unsupported execution type for sources.");
    }

    private static TableSink<?> createTableSink(ExecutionEntry execution, Map<String, String> sinkProperties, ClassLoader classLoader) {
        if (execution.isStreamingPlanner()) {
            final StreamTableSinkFactory<?> factory = (StreamTableSinkFactory<?>)
                    TableFactoryService.find(StreamTableSinkFactory.class, sinkProperties, classLoader);
            return factory.createStreamTableSink(sinkProperties);
        } else if (execution.isBatchPlanner()) {
            final BatchTableSinkFactory<?> factory = (BatchTableSinkFactory<?>)
                    TableFactoryService.find(BatchTableSinkFactory.class, sinkProperties, classLoader);
            return factory.createBatchTableSink(sinkProperties);
        }
        throw new SqlExecutionException("Unsupported execution type for sinks.");
    }


    public ExecutionContext.EnvironmentInstance createEnvironmentInstance() {
        if (environmentInstance != null) {
            return environmentInstance;
        }
        try {
            return new ExecutionContext.EnvironmentInstance(DEFAULT_ENABLE_CHECKPOINT, DEFAULT_JOB_RUN_CONFIG);
        } catch (Throwable t) {
            // catch everything such that a wrong environment does not affect invocations
            throw new SqlExecutionException("Could not create environment instance.", t);
        }
    }

    /**
     * Executes the given supplier using the execution context's classloader as thread classloader.
     */
    public <R> R wrapClassLoader(Supplier<R> supplier) {
        try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(classLoader)) {
            return supplier.get();
        }
    }


    public class EnvironmentInstance {

        private final StreamExecutionEnvironment streamExecEnv;
        private final TableEnvironment tableEnv;
        private Executor executor;

        private EnvironmentInstance(Boolean enableCheckpoint, JobRunConfig jobRunConfig) {

            EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

            // create environments
            streamExecEnv = createStreamExecutionEnvironment(enableCheckpoint, jobRunConfig.getCheckpointInterval(), jobRunConfig.getDefaultParallelism());
            final Map<String, String> executorProperties = settings.toExecutorProperties();
            executor = lookupExecutor(executorProperties, streamExecEnv);
            tableEnv = createStreamTableEnvironment(streamExecEnv, settings, executor);

            // register catalogs
            catalogs.forEach(tableEnv::registerCatalog);


            // register table sources
            tableSources.forEach(tableEnv::registerTableSource);

            // register table sinks
            tableSinks.forEach(tableEnv::registerTableSink);

            if (mergedEnv.getExecution().getCurrentCatalog().isPresent()) {
                tableEnv.useCatalog(mergedEnv.getExecution().getCurrentCatalog().get());
            }


        }

        private Executor lookupExecutor(
                Map<String, String> executorProperties,
                StreamExecutionEnvironment executionEnvironment) {
            try {
                ExecutorFactory executorFactory = ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
                Method createMethod = executorFactory.getClass()
                        .getMethod("create", Map.class, StreamExecutionEnvironment.class);

                return (Executor) createMethod.invoke(
                        executorFactory,
                        executorProperties,
                        executionEnvironment);
            } catch (Exception e) {
                throw new TableException(
                        "Could not instantiate the executor. Make sure a planner module is on the classpath",
                        e);
            }
        }


        public TableEnvironment getTableEnvironment() {
            return tableEnv;
        }

        public ExecutionConfig getExecutionConfig() {
            return streamExecEnv.getConfig();
        }

        public JobGraph createJobGraph(String name) {


            StreamGraph streamGraph = streamExecEnv.getStreamGraph(name);

            List<URL> dependenciesURLs;
            try {
                dependenciesURLs = JarUtils.toURLs(dependencies);
            } catch (MalformedURLException e) {
                throw new UncheckedIOException("convert files to urls error: " + dependencies, e);
            }

            if (executor instanceof ExecutorBase) {
                streamGraph = ((ExecutorBase) executor).getStreamGraph(name);
            }

            final JobGraph jobGraph = ExecutorUtils.getJobGraph(streamGraph, flinkConfig);

            for (URL dependenciesURL : dependenciesURLs) {
                try {
                    jobGraph.addJar(new Path(dependenciesURL.toURI()));
                } catch (URISyntaxException e) {
                    throw new RuntimeException("URL is invalid. This should not happen.", e);
                }

            }

            return jobGraph;

        }

        private SavepointRestoreSettings getSavepointRestoreSettings(String jobName) {
            org.apache.hadoop.fs.Path appCheckpointPath = new org.apache.hadoop.fs.Path(CHECKPOINT_PATH, jobName);
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            SavepointRestoreSettings savepointRestoreSettings = null;
            try {
                FileSystem fileSystem = FileSystem.get(conf);
                if (!fileSystem.exists(appCheckpointPath)) {
                    // 没有app对应的checkpoint则不处理
                    LOG.info("没有找到checkpoint文件");
                    return null;
                }
                List<FileStatus> appCheckDirFiles = Stream.of(fileSystem.listStatus(new org.apache.hadoop.fs.Path(CHECKPOINT_PATH, jobName)))
                        .flatMap(file -> {
                            FileStatus[] fileStatuses = null;
                            try {
                                fileStatuses = fileSystem.listStatus(file.getPath());
                            } catch (IOException e) {
                                LOG.error("查看文件列表失败", e);
                            }
                            return Stream.of(fileStatuses);
                        })
                        .filter(file -> file.getPath().getName().startsWith(AbstractFsCheckpointStorage.CHECKPOINT_DIR_PREFIX))
                        .sorted((x, y) -> Long.compare(y.getModificationTime(), x.getModificationTime()))
                        .collect(Collectors.toList());
                for (FileStatus fileStatus : appCheckDirFiles) {
                    org.apache.hadoop.fs.Path metadataFile = new org.apache.hadoop.fs.Path(fileStatus.getPath().toString(), METADATA_FILE_NAME);
                    if (fileSystem.exists(metadataFile)) {
                        //allowNonRestoredState （可选）：布尔值，指定如果保存点包含无法映射回作业的状态，是否应拒绝作业提交。 default is false
                        LOG.info("Find Savepoint {}", metadataFile);
                        savepointRestoreSettings = SavepointRestoreSettings.forPath(fileStatus.getPath().toString(), true);
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.error("获取filsystem失败", e);
            }

            return savepointRestoreSettings;
        }


        private StreamExecutionEnvironment createStreamExecutionEnvironment(Boolean enableCheckpoint, Long interval, Integer defaultParallelism) {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(10000)));
            env.setParallelism(1);
            env.setMaxParallelism(6);

            if (Objects.nonNull(enableCheckpoint) && enableCheckpoint) {
                if (Objects.nonNull(interval) && interval > 0) {
                    env.enableCheckpointing(interval);
                } else {
                    env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
                }
            }

            if (Objects.nonNull(defaultParallelism)) {
                env.setParallelism(defaultParallelism);
            }

            return env;
        }

        private TableEnvironment createStreamTableEnvironment(
                StreamExecutionEnvironment env,
                EnvironmentSettings settings,
                Executor executor) {

            final TableConfig config = TableConfig.getDefault();

            final CatalogManager catalogManager = new CatalogManager(
                    settings.getBuiltInCatalogName(),
                    new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));

            ModuleManager moduleManager = new ModuleManager();
            FunctionCatalog functionCatalog = new FunctionCatalog(config, catalogManager, moduleManager);

            final Map<String, String> plannerProperties = settings.toPlannerProperties();
            final Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                    .create(plannerProperties, executor, config, functionCatalog, catalogManager);

            return new StreamTableEnvironmentImpl(
                    catalogManager,
                    moduleManager,
                    functionCatalog,
                    config,
                    env,
                    planner,
                    executor,
                    settings.isStreamingMode()
            );
        }

    }
}
