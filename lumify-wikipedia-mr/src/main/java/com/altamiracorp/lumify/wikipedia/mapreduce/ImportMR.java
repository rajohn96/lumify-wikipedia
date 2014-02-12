package com.altamiracorp.lumify.wikipedia.mapreduce;

import com.altamiracorp.bigtable.model.accumulo.AccumuloSession;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.OntologyRepository;
import com.altamiracorp.lumify.core.model.ontology.Relationship;
import com.altamiracorp.lumify.core.model.termMention.TermMentionModel;
import com.altamiracorp.lumify.core.model.termMention.TermMentionRowKey;
import com.altamiracorp.lumify.core.model.workQueue.WorkQueueRepository;
import com.altamiracorp.lumify.core.user.DefaultUserProvider;
import com.altamiracorp.lumify.core.user.UserProvider;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.model.bigtablequeue.BigTableWorkQueueRepository;
import com.altamiracorp.lumify.model.bigtablequeue.model.QueueItem;
import com.altamiracorp.lumify.wikipedia.InternalLinkWithOffsets;
import com.altamiracorp.lumify.wikipedia.TextConverter;
import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.AccumuloAuthorizations;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.accumulo.AccumuloGraphConfiguration;
import com.altamiracorp.securegraph.accumulo.mapreduce.AccumuloElementOutputFormat;
import com.altamiracorp.securegraph.accumulo.mapreduce.ElementMapper;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.MapUtils;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
        private String highlightWorkQueueTableName;
        private AccumuloGraph graph;
        private OntologyRepository ontologyRepository;

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
            UserProvider userProvider = new DefaultUserProvider();
            this.ontologyRepository = new OntologyRepository(graph, userProvider);
            this.visibility = new Visibility("");
            this.authorizations = new AccumuloAuthorizations();

            Concept wikipediaPageConcept = ontologyRepository.getConceptByName("wikipediaPage");
            if (wikipediaPageConcept == null) {
                throw new RuntimeException("wikipediaPage concept not found");
            }
            this.wikipediaPageConceptId = wikipediaPageConcept.getId();

            Relationship wikipediaPageInternalLinkWikipediaPageRelationship = ontologyRepository.getRelationship("wikipediaPageInternalLinkWikipediaPage");
            if (wikipediaPageInternalLinkWikipediaPageRelationship == null) {
                throw new RuntimeException("wikipediaPageInternalLinkWikipediaPage concept not found");
            }
            this.wikipediaPageInternalLinkWikipediaPageRelationshipId = wikipediaPageInternalLinkWikipediaPageRelationship.getId();

            String tablePrefix = BigTableWorkQueueRepository.getTablePrefix(configurationMap);
            this.highlightWorkQueueTableName = BigTableWorkQueueRepository.getTableName(tablePrefix, WorkQueueRepository.ARTIFACT_HIGHLIGHT_QUEUE_NAME);
            try {
                if (!this.graph.getConnector().tableOperations().exists(this.highlightWorkQueueTableName)) {
                    this.graph.getConnector().tableOperations().create(this.highlightWorkQueueTableName);
                }
            } catch (Exception ex) {
                throw new IOException("Could not create table: " + this.highlightWorkQueueTableName, ex);
            }

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

            for (InternalLinkWithOffsets link : textConverter.getInternalLinks()) {
                String linkVertexId = getWikipediaPageVertexId(link.getLink().getTarget());
                VertexBuilder linkedPageVertexBuilder = prepareVertex(linkVertexId, visibility, authorizations);
                CONCEPT_TYPE.setProperty(linkedPageVertexBuilder, wikipediaPageConceptId, visibility);
                MIME_TYPE.setProperty(linkedPageVertexBuilder, WIKIPEDIA_MIME_TYPE, visibility);
                SOURCE.setProperty(linkedPageVertexBuilder, WIKIPEDIA_SOURCE, visibility);
                TITLE.addPropertyValue(linkedPageVertexBuilder, TITLE_LOW_PRIORITY, link.getLink().getTarget(), visibility);
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
                        .setConceptGraphVertexId(wikipediaPageConceptId)
                        .setSign(link.getLink().getTarget())
                        .setVertexId(linkedPageVertex.getId().toString())
                        .setOntologyClassUri(WIKIPEDIA_PAGE_CONCEPT_NAME);
                context.write(new Text(TermMentionModel.TABLE_NAME), AccumuloSession.createMutationFromRow(termMention));
            }

            QueueItem workQueueItem = BigTableWorkQueueRepository.createVertexIdQueueItem(highlightWorkQueueTableName, pageVertex.getId());
            context.write(new Text(highlightWorkQueueTableName), AccumuloSession.createMutationFromRow(workQueueItem));
        }

        private String textToString(org.jdom2.Text text) {
            if (text == null) {
                return "";
            }
            return text.getText();
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

        Job job = new Job(conf, "wikipediaImport");

        String instanceName = accumuloGraphConfiguration.getAccumuloInstanceName();
        String zooKeepers = accumuloGraphConfiguration.getZookeeperServers();
        String principal = accumuloGraphConfiguration.getAccumuloUsername();
        AuthenticationToken authorizationToken = accumuloGraphConfiguration.getAuthenticationToken();
        AccumuloElementOutputFormat.setOutputInfo(job, instanceName, zooKeepers, principal, authorizationToken);

        job.setJarByClass(ImportMR.class);
        job.setMapperClass(ImportMRMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AccumuloElementOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(conf.get("in")));
        return job.waitForCompletion(true) ? 0 : 1;
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
