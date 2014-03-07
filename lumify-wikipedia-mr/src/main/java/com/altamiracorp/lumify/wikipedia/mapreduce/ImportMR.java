package com.altamiracorp.lumify.wikipedia.mapreduce;

import com.altamiracorp.bigtable.model.accumulo.AccumuloSession;
import com.altamiracorp.lumify.core.model.lock.LockRepository;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.OntologyRepository;
import com.altamiracorp.lumify.core.model.ontology.Relationship;
import com.altamiracorp.lumify.core.model.termMention.TermMentionModel;
import com.altamiracorp.lumify.core.model.termMention.TermMentionRowKey;
import com.altamiracorp.lumify.core.model.user.AccumuloAuthorizationRepository;
import com.altamiracorp.lumify.core.model.user.AuthorizationRepository;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.wikipedia.InternalLinkWithOffsets;
import com.altamiracorp.lumify.wikipedia.LinkWithOffsets;
import com.altamiracorp.lumify.wikipedia.RedirectWithOffsets;
import com.altamiracorp.lumify.wikipedia.TextConverter;
import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.AccumuloAuthorizations;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.accumulo.AccumuloGraphConfiguration;
import com.altamiracorp.securegraph.accumulo.mapreduce.AccumuloElementOutputFormat;
import com.altamiracorp.securegraph.accumulo.mapreduce.ElementMapper;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.JoinIterable;
import com.altamiracorp.securegraph.util.MapUtils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.sweble.wikitext.engine.CompiledPage;
import org.sweble.wikitext.engine.Compiler;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.altamiracorp.lumify.core.model.ontology.OntologyLumifyProperties.CONCEPT_TYPE;
import static com.altamiracorp.lumify.core.model.properties.EntityLumifyProperties.SOURCE;
import static com.altamiracorp.lumify.core.model.properties.LumifyProperties.TITLE;
import static com.altamiracorp.lumify.core.model.properties.RawLumifyProperties.*;

public class ImportMR extends Configured implements Tool {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(ImportMR.class);
    public static final String WIKIPEDIA_MIME_TYPE = "text/plain";
    public static final String WIKIPEDIA_SOURCE = "Wikipedia";
    public static final String TITLE_HIGH_PRIORITY = "0";
    public static final String TITLE_LOW_PRIORITY = "2";
    public static final String WIKIPEDIA_ID_PREFIX = "WIKIPEDIA_";
    public static final String WIKIPEDIA_LINK_ID_PREFIX = "WIKIPEDIA_LINK_";
    public static final String WIKIPEDIA_PAGE_CONCEPT_NAME = "wikipediaPage";
    public static final String CONFIG_WIKIPEDIA_PAGE_CONCEPT_ID = "wikipediaPageConceptId";
    public static final String CONFIG_WIKIPEDIA_PAGE_INTERNAL_WIKIPEDIA_PAGE_RELATIONSHIP_ID = "wikipediaPageInternalLinkWikipediaPageRelationshipId";
    public static final char KEY_SPLIT = '\u001f';

    public static class ImportMRMapper extends ElementMapper<LongWritable, Text> {
        public static final String TEXT_XPATH = "/page/revision/text/text()";
        public static final String TITLE_XPATH = "/page/title/text()";
        public static final String REVISION_TIMESTAMP_XPATH = "/page/revision/timestamp/text()";
        public static final SimpleDateFormat ISO8601DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        private XPathExpression<org.jdom2.Text> textXPath;
        private XPathExpression<org.jdom2.Text> titleXPath;
        private XPathExpression<org.jdom2.Text> revisionTimestampXPath;
        private Visibility visibility;
        private Authorizations authorizations;
        private String wikipediaPageConceptId;
        private String wikipediaPageInternalLinkWikipediaPageRelationshipId;
        private Map configurationMap;
        private SimpleWikiConfiguration config;
        private Compiler compiler;
        private AccumuloGraph graph;

        public ImportMRMapper() {
            this.textXPath = XPathFactory.instance().compile(TEXT_XPATH, Filters.text());
            this.titleXPath = XPathFactory.instance().compile(TITLE_XPATH, Filters.text());
            this.revisionTimestampXPath = XPathFactory.instance().compile(REVISION_TIMESTAMP_XPATH, Filters.text());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.configurationMap = toMap(context.getConfiguration());
            this.graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(this.configurationMap, "graph"));
            this.visibility = new Visibility("");
            this.authorizations = new AccumuloAuthorizations();
            this.wikipediaPageConceptId = context.getConfiguration().get(CONFIG_WIKIPEDIA_PAGE_CONCEPT_ID);
            this.wikipediaPageInternalLinkWikipediaPageRelationshipId = context.getConfiguration().get(CONFIG_WIKIPEDIA_PAGE_INTERNAL_WIKIPEDIA_PAGE_RELATIONSHIP_ID);

            try {
                config = new SimpleWikiConfiguration("classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml");
                compiler = new Compiler(config);
            } catch (Exception ex) {
                throw new IOException("Could not configure sweble", ex);
            }
        }

        @Override
        protected IdGenerator getIdGenerator() {
            return this.graph.getIdGenerator();
        }

        @Override
        protected void map(LongWritable filePosition, Text line, Context context) throws IOException, InterruptedException {
            String wikitext;
            String pageTitle;
            Date revisionTimestamp = null;
            String pageString = line.toString().replaceAll("\\\\n", "\n");
            try {
                SAXBuilder builder = new SAXBuilder();
                Document doc = builder.build(new ByteArrayInputStream(pageString.getBytes()));
                pageTitle = textToString(titleXPath.evaluateFirst(doc));
                wikitext = textToString(textXPath.evaluateFirst(doc));
                String revisionTimestampString = textToString(revisionTimestampXPath.evaluateFirst(doc));
                try {
                    revisionTimestamp = ISO8601DATEFORMAT.parse(revisionTimestampString);
                } catch (Exception ex) {
                    LOGGER.error("Could not parse revision timestamp %s", revisionTimestampString, ex);
                }
            } catch (JDOMException e) {
                LOGGER.error("Could not parse XML: " + filePosition + ":\n" + pageString, e);
                return;
            }

            String wikipediaPageVertexId = getWikipediaPageVertexId(pageTitle);
            context.setStatus(wikipediaPageVertexId);

            TextConverter textConverter;
            try {
                String fileTitle = wikipediaPageVertexId;
                PageId pageId = new PageId(PageTitle.make(config, fileTitle), -1);
                CompiledPage compiledPage = compiler.postprocess(pageId, wikitext, null);
                textConverter = new TextConverter(config);
                String text = (String) textConverter.go(compiledPage.getPage());
                if (text.length() > 0) {
                    wikitext = text;
                }
            } catch (Exception ex) {
                LOGGER.error("Could not process wikipedia text: " + filePosition + ":\n" + wikitext, ex);
                return;
            }

            StreamingPropertyValue rawPropertyValue = new StreamingPropertyValue(new ByteArrayInputStream(pageString.getBytes()), byte[].class);
            rawPropertyValue.store(true);
            rawPropertyValue.searchIndex(false);

            StreamingPropertyValue textPropertyValue = new StreamingPropertyValue(new ByteArrayInputStream(wikitext.getBytes()), String.class);

            VertexBuilder pageVertexBuilder = prepareVertex(wikipediaPageVertexId, visibility, authorizations);
            CONCEPT_TYPE.setProperty(pageVertexBuilder, wikipediaPageConceptId, visibility);
            RAW.setProperty(pageVertexBuilder, rawPropertyValue, visibility);
            TITLE.addPropertyValue(pageVertexBuilder, TITLE_HIGH_PRIORITY, pageTitle, visibility);
            MIME_TYPE.setProperty(pageVertexBuilder, WIKIPEDIA_MIME_TYPE, visibility);
            SOURCE.setProperty(pageVertexBuilder, WIKIPEDIA_SOURCE, visibility);
            if (revisionTimestamp != null) {
                PUBLISHED_DATE.setProperty(pageVertexBuilder, revisionTimestamp, visibility);
            }
            TEXT.setProperty(pageVertexBuilder, textPropertyValue, visibility);
            Vertex pageVertex = pageVertexBuilder.save();

            for (LinkWithOffsets link : getLinks(textConverter)) {
                String linkTarget = link.getLinkTargetWithoutHash();
                String linkVertexId = getWikipediaPageVertexId(linkTarget);
                VertexBuilder linkedPageVertexBuilder = prepareVertex(linkVertexId, visibility, authorizations);
                CONCEPT_TYPE.setProperty(linkedPageVertexBuilder, wikipediaPageConceptId, visibility);
                MIME_TYPE.setProperty(linkedPageVertexBuilder, WIKIPEDIA_MIME_TYPE, visibility);
                SOURCE.setProperty(linkedPageVertexBuilder, WIKIPEDIA_SOURCE, visibility);
                TITLE.addPropertyValue(linkedPageVertexBuilder, TITLE_LOW_PRIORITY, linkTarget, visibility);
                Vertex linkedPageVertex = linkedPageVertexBuilder.save();
                addEdge(
                        getWikipediaPageToPageEdgeId(pageVertex, linkedPageVertex),
                        pageVertex,
                        linkedPageVertex,
                        wikipediaPageInternalLinkWikipediaPageRelationshipId,
                        visibility,
                        authorizations);

                TermMentionModel termMention = new TermMentionModel(new TermMentionRowKey(pageVertex.getId().toString(), link.getStartOffset(),
                        link.getEndOffset()));
                termMention.getMetadata()
                        .setConceptGraphVertexId(wikipediaPageConceptId, visibility)
                        .setSign(linkTarget, visibility)
                        .setVertexId(linkedPageVertex.getId().toString(), visibility)
                        .setOntologyClassUri(WIKIPEDIA_PAGE_CONCEPT_NAME, visibility);
                context.write(getKey(TermMentionModel.TABLE_NAME, termMention.getRowKey().toString().getBytes()), AccumuloSession.createMutationFromRow(termMention));
            }
        }

        private Iterable<LinkWithOffsets> getLinks(TextConverter textConverter) {
            return new JoinIterable<LinkWithOffsets>(
                    new ConvertingIterable<InternalLinkWithOffsets, LinkWithOffsets>(textConverter.getInternalLinks()) {
                        @Override
                        protected LinkWithOffsets convert(InternalLinkWithOffsets internalLinkWithOffsets) {
                            return internalLinkWithOffsets;
                        }
                    },
                    new ConvertingIterable<RedirectWithOffsets, LinkWithOffsets>(textConverter.getRedirects()) {
                        @Override
                        protected LinkWithOffsets convert(RedirectWithOffsets redirectWithOffsets) {
                            return redirectWithOffsets;
                        }
                    }
            );
        }

        private String textToString(org.jdom2.Text text) {
            if (text == null) {
                return "";
            }
            return text.getText();
        }

        @Override
        protected void saveDataMutation(Context context, Text dataTableName, Mutation m) throws IOException, InterruptedException {
            context.write(getKey(dataTableName.toString(), m.getRow()), m);
        }

        @Override
        protected void saveEdgeMutation(Context context, Text edgesTableName, Mutation m) throws IOException, InterruptedException {
            context.write(getKey(edgesTableName.toString(), m.getRow()), m);
        }

        @Override
        protected void saveVertexMutation(Context context, Text verticesTableName, Mutation m) throws IOException, InterruptedException {
            context.write(getKey(verticesTableName.toString(), m.getRow()), m);
        }
    }

    public static class ImportMRReducer extends Reducer<Text, Mutation, Text, Mutation> {
        @Override
        protected void reduce(Text keyText, Iterable<Mutation> values, Context context) throws IOException, InterruptedException {
            String key = keyText.toString();
            context.setStatus(key);
            int keySplitLocation = key.indexOf(KEY_SPLIT);
            if (keySplitLocation < 0) {
                throw new IOException("Invalid key: " + keyText);
            }
            String tableNameString = key.substring(0, keySplitLocation);
            Text tableName = new Text(tableNameString);
            for (Mutation m : values) {
                context.write(tableName, m);
            }
        }
    }

    public static String getWikipediaPageVertexId(String pageTitle) {
        return WIKIPEDIA_ID_PREFIX + pageTitle;
    }

    private static String getWikipediaPageToPageEdgeId(Vertex pageVertex, Vertex linkedPageVertex) {
        return WIKIPEDIA_LINK_ID_PREFIX + getWikipediaPageTitleFromId(pageVertex.getId()) + "_" + getWikipediaPageTitleFromId(linkedPageVertex.getId());
    }

    private static String getWikipediaPageTitleFromId(Object id) {
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

        String wikipediaPageConceptId = getWikipediaPageConceptId(ontologyRepository);
        conf.set(CONFIG_WIKIPEDIA_PAGE_CONCEPT_ID, wikipediaPageConceptId);

        String wikipediaPageInternalLinkWikipediaPageRelationshipId = getWikipediaPageInternalLinkWikipediaPageRelationshipId(ontologyRepository);
        conf.set(CONFIG_WIKIPEDIA_PAGE_INTERNAL_WIKIPEDIA_PAGE_RELATIONSHIP_ID, wikipediaPageInternalLinkWikipediaPageRelationshipId);

        Job job = new Job(conf, "wikipediaImport");

        String instanceName = accumuloGraphConfiguration.getAccumuloInstanceName();
        String zooKeepers = accumuloGraphConfiguration.getZookeeperServers();
        String principal = accumuloGraphConfiguration.getAccumuloUsername();
        AuthenticationToken authorizationToken = accumuloGraphConfiguration.getAuthenticationToken();
        AccumuloElementOutputFormat.setOutputInfo(job, instanceName, zooKeepers, principal, authorizationToken);

        if (job.getConfiguration().get("mapred.job.tracker").equals("local")) {
            LOGGER.warn("!!!!!! Running in local mode !!!!!!");
        } else {
            List<Text> splits = new ArrayList<Text>();
            splits.addAll(getSplits(graph, graph.getVerticesTableName()));
            splits.addAll(getSplits(graph, graph.getEdgesTableName()));
            splits.addAll(getSplits(graph, graph.getDataTableName()));
            splits.addAll(getSplits(graph, TermMentionModel.TABLE_NAME));
            Collections.sort(splits);

            Path splitFile = new Path("/tmp/wikipediaImport_splits.txt");
            FileSystem fs = FileSystem.get(conf);
            PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitFile)));
            for (Text split : splits) {
                out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
            }
            out.close();

            job.setPartitionerClass(RangePartitioner.class);
            RangePartitioner.setSplitFile(job, splitFile.toString());
            job.setNumReduceTasks(splits.size() + 1);
        }

        job.setJarByClass(ImportMR.class);
        job.setMapperClass(ImportMRMapper.class);
        job.setReducerClass(ImportMRReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AccumuloElementOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(conf.get("in")));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Collection<Text> getSplits(AccumuloGraph graph, String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        Collection<Text> splits = graph.getConnector().tableOperations().listSplits(tableName, 100);
        if (splits.size() == 0) {
            throw new RuntimeException("Could not find splits on table: " + tableName);
        }
        List<Text> tableNamePrefixedSplits = new ArrayList<Text>();
        for (Text split : splits) {
            Text splitName = getKey(tableName, TextUtil.getBytes(split));
            tableNamePrefixedSplits.add(splitName);
        }
        return tableNamePrefixedSplits;
    }

    private static Text getKey(String tableName, byte[] key) {
        return new Text(tableName + KEY_SPLIT + new String(Base64.encodeBase64(key)));
    }

    private String getWikipediaPageInternalLinkWikipediaPageRelationshipId(OntologyRepository ontologyRepository) {
        Relationship wikipediaPageInternalLinkWikipediaPageRelationship = ontologyRepository.getRelationship("wikipediaPageInternalLinkWikipediaPage");
        if (wikipediaPageInternalLinkWikipediaPageRelationship == null) {
            throw new RuntimeException("wikipediaPageInternalLinkWikipediaPage concept not found");
        }
        return wikipediaPageInternalLinkWikipediaPageRelationship.getId();
    }

    private String getWikipediaPageConceptId(OntologyRepository ontologyRepository) {
        Concept wikipediaPageConcept = ontologyRepository.getConceptByName("wikipediaPage");
        if (wikipediaPageConcept == null) {
            throw new RuntimeException("wikipediaPage concept not found");
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
