package com.yiwei.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yiwei  2020/4/5
 */
@SuppressWarnings({"SameParameterValue", "rawtypes"})
public class FlinkCliFrontend {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCliFrontend.class);


    // configuration dir parameters
    private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
    private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

    // --------------------------------------------------------------------------------------------

    private final Configuration configuration;

    private List<CustomCommandLine> customCommandLines;

    private final Options customCommandLineOptions;

    private final int defaultParallelism;

    public FlinkCliFrontend(
            Configuration configuration) {
        this.configuration = Preconditions.checkNotNull(configuration);

        FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        this.customCommandLineOptions = new Options();

        this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
    }

    public FlinkCliFrontend(
            Configuration configuration,
            List<CustomCommandLine> customCommandLines) {
        this.configuration = Preconditions.checkNotNull(configuration);
        this.customCommandLines = Preconditions.checkNotNull(customCommandLines);

        FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        this.customCommandLineOptions = new Options();

        for (CustomCommandLine customCommandLine : customCommandLines) {
            customCommandLine.addGeneralOptions(customCommandLineOptions);
            customCommandLine.addRunOptions(customCommandLineOptions);
        }

        this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
    }


    // --------------------------------------------------------------------------------------------
    //  Getter & Setter
    // --------------------------------------------------------------------------------------------

    /**
     * Getter which returns a copy of the associated configuration.
     *
     * @return Copy of the associated configuration
     */
    public Configuration getConfiguration() {
        Configuration copiedConfiguration = new Configuration();

        copiedConfiguration.addAll(configuration);

        return copiedConfiguration;
    }

    public Options getCustomCommandLineOptions() {
        return customCommandLineOptions;
    }


    /**
     * Executions the run action.
     *
     * @param args Command line arguments for the run action.
     */
    public Object deploy(String[] args, JobGraph jobGraph) throws Exception {
        LOG.info("Running 'run' command.");

        final Options commandOptions = FlinkCliFrontendParser.getRunCommandOptions();

        final Options commandLineOptions = FlinkCliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

        final CommandLine commandLine = FlinkCliFrontendParser.parse(commandLineOptions, args, true);

        final ProgramOptions runOptions = new ProgramOptions(commandLine);

        // evaluate help flag
        if (runOptions.isPrintHelp()) {
            FlinkCliFrontendParser.printHelpForRun(customCommandLines);
            return null;
        }

        final CustomCommandLine customCommandLine = getActiveCustomCommandLine(commandLine);

        return deployJobGraph(customCommandLine, commandLine, jobGraph);


    }

    private Object deployJobGraph(
            CustomCommandLine customCommandLine,
            CommandLine commandLine,
            JobGraph jobGraph) throws FlinkException {
        final Configuration configuration = customCommandLine.applyCommandLineOptionsToConfiguration(commandLine);
        final YarnClusterClientFactory yarnClusterClientFactory = new YarnClusterClientFactory();
        final ClusterDescriptor clusterDescriptor = yarnClusterClientFactory.createClusterDescriptor(configuration);
        Object clusterId = null;
        try {

            final ClusterClientProvider clientProvider;


            final ClusterSpecification clusterSpecification = yarnClusterClientFactory.getClusterSpecification(configuration);

            clientProvider = clusterDescriptor.deployJobCluster(
                    clusterSpecification,
                    jobGraph,
                    true);

            logAndSysout("Job has been submitted with JobID " + jobGraph.getJobID());

            try {
                final ClusterClient clusterClient = clientProvider.getClusterClient();
                clusterId = clusterClient.getClusterId();
                clusterClient.shutDownCluster();
            } catch (Exception e) {
                LOG.info("Could not properly shut down the clientProvider.", e);
            }

        } finally {
            try {
                clusterDescriptor.close();
            } catch (Exception e) {
                LOG.info("Could not properly close the cluster descriptor.", e);
            }
        }
        return clusterId;
    }




    private static void logAndSysout(String message) {
        LOG.info(message);
        System.out.println(message);
    }


    public static String getConfigurationDirectoryFromEnv() {
        String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

        if (location != null) {
            if (new File(location).exists()) {
                return location;
            } else {
                throw new RuntimeException("The configuration directory '" + location + "', specified in the '" +
                        ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable, does not exist.");
            }
        } else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
            location = CONFIG_DIRECTORY_FALLBACK_1;
        } else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
            location = CONFIG_DIRECTORY_FALLBACK_2;
        } else {
            throw new RuntimeException("The configuration directory was not specified. " +
                    "Please specify the directory containing the configuration file through the '" +
                    ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable.");
        }
        return location;
    }



    public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration) {
        List<CustomCommandLine> customCommandLines = new ArrayList<>(2);
        customCommandLines.add(new DefaultCLI(configuration));
        return customCommandLines;
    }

    public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
        List<CustomCommandLine> customCommandLines = new ArrayList<>(2);

        //	Command line interface of the YARN session, with a special initialization here
        //	to prefix all options with y/yarn.
        //	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
        //	      active CustomCommandLine in order and DefaultCLI isActive always return true.
        final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
        try {
            customCommandLines.add(
                    loadCustomCommandLine(flinkYarnSessionCLI,
                            configuration,
                            configurationDirectory,
                            "y",
                            "yarn"));
        } catch (NoClassDefFoundError | Exception e) {
            LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
        }

        customCommandLines.add(new DefaultCLI(configuration));

        return customCommandLines;
    }

    // --------------------------------------------------------------------------------------------
    //  Custom command-line
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the custom command-line for the arguments.
     *
     * @param commandLine The input to the command-line.
     * @return custom command-line which is active (may only be one at a time)
     */
    public CustomCommandLine getActiveCustomCommandLine(CommandLine commandLine) {
        for (CustomCommandLine cli : customCommandLines) {
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new IllegalStateException("No command-line ran.");
    }

    /**
     * Loads a class from the classpath that implements the CustomCommandLine interface.
     *
     * @param className The fully-qualified class name to load.
     * @param params    The constructor parameters
     */
    private static CustomCommandLine loadCustomCommandLine(String className, Object... params) throws IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException, NoSuchMethodException {

        Class<? extends CustomCommandLine> customCliClass =
                Class.forName(className).asSubclass(CustomCommandLine.class);

        // construct class types from the parameters
        Class[] types = new Class[params.length];
        for (int i = 0; i < params.length; i++) {
            Preconditions.checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
            types[i] = params[i].getClass();
        }

        Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);

        return constructor.newInstance(params);
    }

}
