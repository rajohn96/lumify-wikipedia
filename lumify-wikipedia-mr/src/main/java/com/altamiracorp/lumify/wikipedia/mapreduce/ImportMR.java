package com.altamiracorp.lumify.wikipedia.mapreduce;

import com.altamiracorp.lumify.core.model.lock.LockRepository;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.OntologyRepository;
import com.altamiracorp.lumify.core.model.ontology.Relationship;
import com.altamiracorp.lumify.core.model.termMention.TermMentionModel;
import com.altamiracorp.lumify.core.model.user.AccumuloAuthorizationRepository;
import com.altamiracorp.lumify.core.model.user.AuthorizationRepository;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.wikipedia.WikipediaConstants;
import com.altamiracorp.securegraph.GraphFactory;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.accumulo.AccumuloConstants;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.accumulo.AccumuloGraphConfiguration;
import com.altamiracorp.securegraph.accumulo.mapreduce.AccumuloElementOutputFormat;
import com.altamiracorp.securegraph.accumulo.mapreduce.ElementMapper;
import com.altamiracorp.securegraph.util.MapUtils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

public class ImportMR extends Configured implements Tool {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(ImportMR.class);
    public static final String WIKIPEDIA_MIME_TYPE = "text/plain";
    public static final String WIKIPEDIA_SOURCE = "Wikipedia";
    public static final String TITLE_HIGH_PRIORITY = "0";
    public static final String TITLE_LOW_PRIORITY = "2";
    public static final String WIKIPEDIA_ID_PREFIX = "WIKIPEDIA_";
    public static final String WIKIPEDIA_LINK_ID_PREFIX = "WIKIPEDIA_LINK_";

    static final char KEY_SPLIT = '\u001f';
    static final String TABLE_NAME_ELASTIC_SEARCH = "elasticSearch";

    static String getWikipediaPageVertexId(String pageTitle) {
        return WIKIPEDIA_ID_PREFIX + pageTitle;
    }

    static String getWikipediaPageToPageEdgeId(Vertex pageVertex, Vertex linkedPageVertex) {
        return WIKIPEDIA_LINK_ID_PREFIX + getWikipediaPageTitleFromId(pageVertex.getId()) + "_" + getWikipediaPageTitleFromId(linkedPageVertex.getId());
    }

    static String getWikipediaPageTitleFromId(Object id) {
        return id.toString().substring(WIKIPEDIA_ID_PREFIX.length());
    }

    @Override
    public int run(String[] args) throws Exception {
        com.altamiracorp.lumify.core.config.Configuration lumifyConfig = com.altamiracorp.lumify.core.config.Configuration.loadConfigurationFile();
        Configuration conf = getConfiguration(args, lumifyConfig);
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(conf, "graph.");

        Map configurationMap = toMap(conf);
        AccumuloGraph graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(configurationMap, "graph"));
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String zookeeperConnectionString = conf.get(com.altamiracorp.lumify.core.config.Configuration.ZK_SERVERS);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        curatorFramework.start();
        LockRepository lockRepository = new LockRepository(curatorFramework);
        AuthorizationRepository authorizationRepository = new AccumuloAuthorizationRepository(graph, lockRepository);
        OntologyRepository ontologyRepository = new OntologyRepository(graph, authorizationRepository);

        verifyWikipediaPageConceptId(ontologyRepository);
        verifyWikipediaPageInternalLinkWikipediaPageRelationshipId(ontologyRepository);

        conf.set(ImportMRReducer.MAX_ITEMS_PER_REQUEST, Integer.toString(ImportMRReducer.DEFAULT_MAX_ITEMS_PER_REQUEST));

        Job job = new Job(conf, "wikipediaImport");

        String instanceName = accumuloGraphConfiguration.getAccumuloInstanceName();
        String zooKeepers = accumuloGraphConfiguration.getZookeeperServers();
        String principal = accumuloGraphConfiguration.getAccumuloUsername();
        AuthenticationToken authorizationToken = accumuloGraphConfiguration.getAuthenticationToken();
        AccumuloElementOutputFormat.setOutputInfo(job, instanceName, zooKeepers, principal, authorizationToken);

        List<Text> splits = getSplits(graph);
        Path splitFile = writeSplitsFile(conf, splits);

        if (job.getConfiguration().get("mapred.job.tracker").equals("local")) {
            LOGGER.warn("!!!!!! Running in local mode !!!!!!");
        } else {
            job.setPartitionerClass(RangePartitioner.class);
            RangePartitioner.setSplitFile(job, splitFile.toString());
            job.setNumReduceTasks(splits.size() + 1);
        }

        job.setJarByClass(ImportMR.class);
        job.setMapperClass(ImportMRMapper.class);
        job.setMapOutputValueClass(MutationOrElasticSearchIndexWritable.class);
        job.setReducerClass(ImportMRReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AccumuloElementOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(conf.get("in")));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Path writeSplitsFile(Configuration conf, List<Text> splits) throws IOException {
        Path splitFile = new Path("/tmp/wikipediaImport_splits.txt");
        FileSystem fs = FileSystem.get(conf);
        PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitFile)));
        for (Text split : splits) {
            out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
        }
        out.close();
        return splitFile;
    }

    private List<Text> getSplits(AccumuloGraph graph) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        List<Text> splits = new ArrayList<Text>();
        splits.addAll(getSplits(graph, graph.getVerticesTableName()));
        splits.addAll(getSplits(graph, graph.getEdgesTableName()));
        splits.addAll(getSplits(graph, graph.getDataTableName()));
        splits.addAll(getSplits(graph, TermMentionModel.TABLE_NAME));
        splits.addAll(getSplits(graph, TABLE_NAME_ELASTIC_SEARCH));
        Collections.sort(splits);
        return splits;
    }

    private Collection<Text> getSplits(AccumuloGraph graph, String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        boolean forEs = false;

        if (TABLE_NAME_ELASTIC_SEARCH.equals(tableName)) {
            forEs = true;
            tableName = graph.getVerticesTableName();
        }

        List<Text> tableNamePrefixedSplits = new ArrayList<Text>();
        Collection<Text> splits = graph.getConnector().tableOperations().listSplits(tableName, 100);
        if (splits.size() == 0) {
            return tableNamePrefixedSplits;
        }
        for (Text split : splits) {
            if (forEs) {
                Text splitName = getKey(TABLE_NAME_ELASTIC_SEARCH, split.toString().substring(AccumuloConstants.VERTEX_ROW_KEY_PREFIX.length()).getBytes());
                tableNamePrefixedSplits.add(splitName);
            } else {
                Text splitName = getKey(tableName, TextUtil.getBytes(split));
                tableNamePrefixedSplits.add(splitName);
            }
        }
        return tableNamePrefixedSplits;
    }

    static Text getKey(String tableName, byte[] key) {
        return new Text(tableName + KEY_SPLIT + new String(Base64.encodeBase64(key)));
    }

    private String verifyWikipediaPageInternalLinkWikipediaPageRelationshipId(OntologyRepository ontologyRepository) {
        Relationship wikipediaPageInternalLinkWikipediaPageRelationship = ontologyRepository.getRelationshipById(WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI);
        if (wikipediaPageInternalLinkWikipediaPageRelationship == null) {
            throw new RuntimeException(WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI + " concept not found");
        }
        return wikipediaPageInternalLinkWikipediaPageRelationship.getId();
    }

    private String verifyWikipediaPageConceptId(OntologyRepository ontologyRepository) {
        Concept wikipediaPageConcept = ontologyRepository.getConceptById(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI);
        if (wikipediaPageConcept == null) {
            throw new RuntimeException(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI + " concept not found");
        }
        return wikipediaPageConcept.getId();
    }

    private Configuration getConfiguration(String[] args, com.altamiracorp.lumify.core.config.Configuration lumifyConfig) {
        if (args.length != 1) {
            throw new RuntimeException("Required arguments <inputFileName>");
        }
        String inFileName = args[0];
        LOGGER.info("Using config:\n" + lumifyConfig);

        Configuration hadoopConfig = lumifyConfig.toHadoopConfiguration();
        hadoopConfig.set(ElementMapper.GRAPH_CONFIG_PREFIX, "graph.");
        LOGGER.info("inFileName: %s", inFileName);
        hadoopConfig.set("in", inFileName);
        this.setConf(hadoopConfig);
        return hadoopConfig;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImportMR(), args);
        System.exit(res);
    }

    protected static Map toMap(Configuration configuration) {
        Map map = new HashMap();
        for (Map.Entry<String, String> entry : configuration) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
