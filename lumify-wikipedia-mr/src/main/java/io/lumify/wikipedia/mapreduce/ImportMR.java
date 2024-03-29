package io.lumify.wikipedia.mapreduce;

import com.google.inject.Inject;
import io.lumify.core.bootstrap.InjectHelper;
import io.lumify.core.bootstrap.LumifyBootstrap;
import io.lumify.core.model.ontology.Concept;
import io.lumify.core.model.ontology.OntologyRepository;
import io.lumify.core.model.ontology.Relationship;
import io.lumify.core.model.termMention.TermMentionModel;
import io.lumify.core.util.LumifyLogger;
import io.lumify.core.util.LumifyLoggerFactory;
import io.lumify.wikipedia.WikipediaConstants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.securegraph.Graph;
import org.securegraph.Vertex;
import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.accumulo.AccumuloGraphConfiguration;
import org.securegraph.accumulo.mapreduce.AccumuloElementOutputFormat;
import org.securegraph.accumulo.mapreduce.ElementMapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ImportMR extends Configured implements Tool {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(ImportMR.class);
    public static final String WIKIPEDIA_MIME_TYPE = "text/plain";
    public static final String WIKIPEDIA_SOURCE = "Wikipedia";
    public static final String WIKIPEDIA_ID_PREFIX = "WIKIPEDIA_";
    public static final String WIKIPEDIA_LINK_ID_PREFIX = "WIKIPEDIA_LINK_";
    public static final String MULTI_VALUE_KEY = ImportMR.class.getName();

    static final char KEY_SPLIT = '\u001f';
    private OntologyRepository ontologyRepository;
    private Graph graph;

    static String getWikipediaPageVertexId(String pageTitle) {
        return WIKIPEDIA_ID_PREFIX + pageTitle.trim();
    }

    static String getWikipediaPageToPageEdgeId(Vertex pageVertex, Vertex linkedPageVertex) {
        return WIKIPEDIA_LINK_ID_PREFIX + getWikipediaPageTitleFromId(pageVertex.getId()) + "_" + getWikipediaPageTitleFromId(linkedPageVertex.getId());
    }

    static String getWikipediaPageTitleFromId(Object id) {
        return id.toString().substring(WIKIPEDIA_ID_PREFIX.length());
    }

    @Override
    public int run(String[] args) throws Exception {
        io.lumify.core.config.Configuration lumifyConfig = io.lumify.core.config.Configuration.loadConfigurationFile();
        Configuration conf = getConfiguration(args, lumifyConfig);
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(conf, "graph.");
        InjectHelper.inject(this, LumifyBootstrap.bootstrapModuleMaker(lumifyConfig));

        verifyWikipediaPageConcept(ontologyRepository);
        verifyWikipediaPageInternalLinkWikipediaPageRelationship(ontologyRepository);

        Job job = new Job(conf, "wikipediaImport");

        String instanceName = accumuloGraphConfiguration.getAccumuloInstanceName();
        String zooKeepers = accumuloGraphConfiguration.getZookeeperServers();
        String principal = accumuloGraphConfiguration.getAccumuloUsername();
        AuthenticationToken authorizationToken = accumuloGraphConfiguration.getAuthenticationToken();
        AccumuloElementOutputFormat.setOutputInfo(job, instanceName, zooKeepers, principal, authorizationToken);

        List<Text> splits = getSplits((AccumuloGraph) graph);
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
        job.setMapOutputValueClass(Mutation.class);
        job.setReducerClass(ImportMRReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AccumuloElementOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(conf.get("in")));

        int returnCode = job.waitForCompletion(true) ? 0 : 1;

        CounterGroup groupCounters = job.getCounters().getGroup(WikipediaImportCounters.class.getName());
        for (Counter counter : groupCounters) {
            System.out.println(counter.getDisplayName() + ": " + counter.getValue());
        }

        return returnCode;
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
        Collections.sort(splits);
        return splits;
    }

    private Collection<Text> getSplits(AccumuloGraph graph, String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        List<Text> tableNamePrefixedSplits = new ArrayList<Text>();
        Collection<Text> splits = graph.getConnector().tableOperations().listSplits(tableName, 100);
        if (splits.size() == 0) {
            return tableNamePrefixedSplits;
        }
        for (Text split : splits) {
            Text splitName = getKey(tableName, TextUtil.getBytes(split));
            tableNamePrefixedSplits.add(splitName);
        }
        return tableNamePrefixedSplits;
    }

    static Text getKey(String tableName, byte[] key) {
        return new Text(tableName + KEY_SPLIT + new String(Base64.encodeBase64(key)));
    }

    private void verifyWikipediaPageInternalLinkWikipediaPageRelationship(OntologyRepository ontologyRepository) {
        Relationship wikipediaPageInternalLinkWikipediaPageRelationship = ontologyRepository.getRelationshipByIRI(WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI);
        if (wikipediaPageInternalLinkWikipediaPageRelationship == null) {
            throw new RuntimeException(WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI + " relationship not found");
        }
    }

    private void verifyWikipediaPageConcept(OntologyRepository ontologyRepository) {
        Concept wikipediaPageConcept = ontologyRepository.getConceptByIRI(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI);
        if (wikipediaPageConcept == null) {
            throw new RuntimeException(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI + " concept not found");
        }
    }

    private Configuration getConfiguration(String[] args, io.lumify.core.config.Configuration lumifyConfig) {
        if (args.length != 1) {
            throw new RuntimeException("Required arguments <inputFileName>");
        }
        String inFileName = args[0];
        LOGGER.info("Using config:\n" + lumifyConfig);

        Configuration hadoopConfig = lumifyConfig.toHadoopConfiguration();
        hadoopConfig.set(ElementMapper.GRAPH_CONFIG_PREFIX, "graph.");
        LOGGER.info("inFileName: %s", inFileName);
        hadoopConfig.set("in", inFileName);
        hadoopConfig.set(ImportMRMapper.CONFIG_SOURCE_FILE_NAME, new File(inFileName).getName());
        this.setConf(hadoopConfig);
        return hadoopConfig;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImportMR(), args);
        System.exit(res);
    }

    @Inject
    public void setOntologyRepository(OntologyRepository ontologyRepository) {
        this.ontologyRepository = ontologyRepository;
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
    }
}
