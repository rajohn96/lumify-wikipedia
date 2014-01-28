package com.altamiracorp.lumify.wikipedia;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.storm.StormRunnerBase;

public class StormRunner extends StormRunnerBase {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(StormRunner.class);
    private static final String TOPOLOGY_NAME = "lumify-wikipedia";

    public static void main(String[] args) throws Exception {
        int res = new StormRunner().run(args);
        if (res != 0) {
            System.exit(res);
        }
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