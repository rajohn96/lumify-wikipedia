package com.altamiracorp.lumify.wikipedia.mapreduce;

import com.altamiracorp.bigtable.model.accumulo.AccumuloSession;
import com.altamiracorp.lumify.core.model.termMention.TermMentionModel;
import com.altamiracorp.lumify.core.model.termMention.TermMentionRowKey;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.wikipedia.*;
import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.AccumuloAuthorizations;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.accumulo.mapreduce.ElementMapper;
import com.altamiracorp.securegraph.elasticsearch.ElasticSearchSearchIndex;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.JoinIterable;
import com.altamiracorp.securegraph.util.MapUtils;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.Map;

import static com.altamiracorp.lumify.core.model.ontology.OntologyLumifyProperties.CONCEPT_TYPE;
import static com.altamiracorp.lumify.core.model.properties.EntityLumifyProperties.SOURCE;
import static com.altamiracorp.lumify.core.model.properties.LumifyProperties.TITLE;
import static com.altamiracorp.lumify.core.model.properties.RawLumifyProperties.*;

class ImportMRMapper extends ElementMapper<LongWritable, Text, Text, MutationOrElasticSearchIndexWritable> {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(ImportMRMapper.class);
    public static final String TEXT_XPATH = "/page/revision/text/text()";
    public static final String TITLE_XPATH = "/page/title/text()";
    public static final String REVISION_TIMESTAMP_XPATH = "/page/revision/timestamp/text()";
    public static final SimpleDateFormat ISO8601DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private XPathExpression<org.jdom2.Text> textXPath;
    private XPathExpression<org.jdom2.Text> titleXPath;
    private XPathExpression<org.jdom2.Text> revisionTimestampXPath;
    private Visibility visibility;
    private Authorizations authorizations;
    private SimpleWikiConfiguration config;
    private org.sweble.wikitext.engine.Compiler compiler;
    private AccumuloGraph graph;
    private ElasticSearchSearchIndex searchIndex;

    public ImportMRMapper() {
        this.textXPath = XPathFactory.instance().compile(TEXT_XPATH, Filters.text());
        this.titleXPath = XPathFactory.instance().compile(TITLE_XPATH, Filters.text());
        this.revisionTimestampXPath = XPathFactory.instance().compile(REVISION_TIMESTAMP_XPATH, Filters.text());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Map configurationMap = ImportMR.toMap(context.getConfiguration());
        this.graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(configurationMap, "graph"));
        this.visibility = new Visibility("");
        this.authorizations = new AccumuloAuthorizations();
        this.searchIndex = (ElasticSearchSearchIndex) this.graph.getSearchIndex();

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
        try {
            safeMap(filePosition, line, context);
        } catch (Exception ex) {
            LOGGER.error("failed mapping " + filePosition, ex);
        }
    }

    private void safeMap(LongWritable filePosition, Text line, Context context) throws IOException, InterruptedException {
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

        String wikipediaPageVertexId = ImportMR.getWikipediaPageVertexId(pageTitle);
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
        CONCEPT_TYPE.setProperty(pageVertexBuilder, WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, visibility);
        RAW.setProperty(pageVertexBuilder, rawPropertyValue, visibility);
        TITLE.addPropertyValue(pageVertexBuilder, ImportMR.TITLE_HIGH_PRIORITY, pageTitle, visibility);
        MIME_TYPE.setProperty(pageVertexBuilder, ImportMR.WIKIPEDIA_MIME_TYPE, visibility);
        SOURCE.setProperty(pageVertexBuilder, ImportMR.WIKIPEDIA_SOURCE, visibility);
        if (revisionTimestamp != null) {
            PUBLISHED_DATE.setProperty(pageVertexBuilder, revisionTimestamp, visibility);
        }
        TEXT.setProperty(pageVertexBuilder, textPropertyValue, visibility);
        Vertex pageVertex = pageVertexBuilder.save();

        this.searchIndex.addPropertiesToIndex(pageVertex.getProperties());

        // because save above will cause the StreamingPropertyValue to be read we need to reset the position to 0 for search indexing
        rawPropertyValue.getInputStream().reset();
        textPropertyValue.getInputStream().reset();

        String elasticSearchJson = this.searchIndex.createJsonForElement(pageVertex);
        Text key = ImportMR.getKey(ImportMR.TABLE_NAME_ELASTIC_SEARCH, wikipediaPageVertexId.getBytes());
        context.write(key, new MutationOrElasticSearchIndexWritable(wikipediaPageVertexId, elasticSearchJson));

        for (LinkWithOffsets link : getLinks(textConverter)) {
            String linkTarget = link.getLinkTargetWithoutHash();
            String linkVertexId = ImportMR.getWikipediaPageVertexId(linkTarget);
            VertexBuilder linkedPageVertexBuilder = prepareVertex(linkVertexId, visibility, authorizations);
            CONCEPT_TYPE.setProperty(linkedPageVertexBuilder, WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, visibility);
            MIME_TYPE.setProperty(linkedPageVertexBuilder, ImportMR.WIKIPEDIA_MIME_TYPE, visibility);
            SOURCE.setProperty(linkedPageVertexBuilder, ImportMR.WIKIPEDIA_SOURCE, visibility);
            TITLE.addPropertyValue(linkedPageVertexBuilder, ImportMR.TITLE_LOW_PRIORITY, linkTarget, visibility);
            Vertex linkedPageVertex = linkedPageVertexBuilder.save();
            Edge edge = addEdge(ImportMR.getWikipediaPageToPageEdgeId(pageVertex, linkedPageVertex),
                    pageVertex,
                    linkedPageVertex,
                    WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI,
                    visibility,
                    authorizations);

            TermMentionModel termMention = new TermMentionModel(new TermMentionRowKey(pageVertex.getId().toString(), "", link.getStartOffset(),
                    link.getEndOffset()));
            termMention.getMetadata()
                    .setConceptGraphVertexId(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, visibility)
                    .setSign(linkTarget, visibility)
                    .setVertexId(linkedPageVertex.getId().toString(), visibility)
                    .setEdgeId(edge.getId().toString(), visibility)
                    .setOntologyClassUri(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, visibility);
            key = ImportMR.getKey(TermMentionModel.TABLE_NAME, termMention.getRowKey().toString().getBytes());
            Mutation m = AccumuloSession.createMutationFromRow(termMention);
            context.write(key, new MutationOrElasticSearchIndexWritable(m));
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
        context.write(ImportMR.getKey(dataTableName.toString(), m.getRow()), new MutationOrElasticSearchIndexWritable(m));
    }

    @Override
    protected void saveEdgeMutation(Context context, Text edgesTableName, Mutation m) throws IOException, InterruptedException {
        context.write(ImportMR.getKey(edgesTableName.toString(), m.getRow()), new MutationOrElasticSearchIndexWritable(m));
    }

    @Override
    protected void saveVertexMutation(Context context, Text verticesTableName, Mutation m) throws IOException, InterruptedException {
        context.write(ImportMR.getKey(verticesTableName.toString(), m.getRow()), new MutationOrElasticSearchIndexWritable(m));
    }
}