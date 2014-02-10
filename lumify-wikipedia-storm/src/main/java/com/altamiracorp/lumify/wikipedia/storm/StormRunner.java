package com.altamiracorp.lumify.wikipedia.storm;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.storm.StormRunnerBase;
import com.altamiracorp.lumify.wikipedia.WikipediaConstants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class StormRunner extends StormRunnerBase {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(StormRunner.class);
    private static final String TOPOLOGY_NAME = "lumify-wikipedia";
    private static final String CMD_OPT_NO_FLUSH = "noflush";

    public static void main(String[] args) throws Exception {
        int res = new StormRunner().run(args);
        if (res != 0) {
            System.exit(res);
        }
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();

        opts.addOption(
                OptionBuilder
                        .withLongOpt(CMD_OPT_NO_FLUSH)
                        .withDescription("Don't flush on each record")
                        .create()
        );

        return opts;
    }

    @Override
    protected Config createConfig(CommandLine cmd) {
        Config config = super.createConfig(cmd);
        config.put(WikipediaConstants.CONFIG_FLUSH, !cmd.hasOption(CMD_OPT_NO_FLUSH));
        return config;
    }

    @Override
    protected String getTopologyName() {
        return TOPOLOGY_NAME;
    }

    public StormTopology createTopology(int parallelismHint) {
        TopologyBuilder builder = new TopologyBuilder();
        createTopology(builder, parallelismHint);
        return builder.createTopology();
    }

    private void createTopology(TopologyBuilder builder, int parallelismHint) {
        String name = "wikipedia";
        builder.setSpout(name + "-spout", createWorkQueueRepositorySpout(WikipediaConstants.WIKIPEDIA_QUEUE), 1)
                .setMaxTaskParallelism(1);
        builder.setBolt(name + "-bolt", new WikipediaBolt(), parallelismHint)
                .shuffleGrouping(name + "-spout");
    }
}