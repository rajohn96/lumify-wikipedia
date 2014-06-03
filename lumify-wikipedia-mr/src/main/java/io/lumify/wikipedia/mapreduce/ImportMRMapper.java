package io.lumify.wikipedia.mapreduce;

import com.altamiracorp.bigtable.model.accumulo.AccumuloSession;
import io.lumify.core.model.ontology.OntologyLumifyProperties;
import io.lumify.core.model.properties.EntityLumifyProperties;
import io.lumify.core.model.properties.LumifyProperties;
import io.lumify.core.model.properties.RawLumifyProperties;
import io.lumify.core.model.termMention.TermMentionModel;
import io.lumify.core.model.termMention.TermMentionRowKey;
import io.lumify.core.util.LumifyLogger;
import io.lumify.core.util.LumifyLoggerFactory;
import io.lumify.wikipedia.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.securegraph.*;
import org.securegraph.accumulo.AccumuloAuthorizations;
import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.accumulo.mapreduce.ElementMapper;
import org.securegraph.accumulo.mapreduce.SecureGraphMRUtils;
import org.securegraph.id.IdGenerator;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.JoinIterable;
import org.securegraph.util.MapUtils;
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

class ImportMRMapper extends ElementMapper<LongWritable, Text, Text, Mutation> {
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

    public ImportMRMapper() {
        this.textXPath = XPathFactory.instance().compile(TEXT_XPATH, Filters.text());
        this.titleXPath = XPathFactory.instance().compile(TITLE_XPATH, Filters.text());
        this.revisionTimestampXPath = XPathFactory.instance().compile(REVISION_TIMESTAMP_XPATH, Filters.text());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Map configurationMap = SecureGraphMRUtils.toMap(context.getConfiguration());
        this.graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(configurationMap, "graph"));
        this.visibility = new Visibility("");
        this.authorizations = new AccumuloAuthorizations();

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

        VertexBuilder pageVertexBuilder = prepareVertex(wikipediaPageVertexId, visibility);
        OntologyLumifyProperties.CONCEPT_TYPE.setProperty(pageVertexBuilder, WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, visibility);
        RawLumifyProperties.RAW.setProperty(pageVertexBuilder, rawPropertyValue, visibility);

        Map<String, Object> titleMetadata = new HashMap<String, Object>();
        LumifyProperties.CONFIDENCE.setMetadata(titleMetadata, 0.4);
        LumifyProperties.TITLE.addPropertyValue(pageVertexBuilder, ImportMR.MULTI_VALUE_KEY, pageTitle, titleMetadata, visibility);

        RawLumifyProperties.MIME_TYPE.setProperty(pageVertexBuilder, ImportMR.WIKIPEDIA_MIME_TYPE, visibility);
        EntityLumifyProperties.SOURCE.addPropertyValue(pageVertexBuilder, ImportMR.MULTI_VALUE_KEY, ImportMR.WIKIPEDIA_SOURCE, visibility);
        if (revisionTimestamp != null) {
            RawLumifyProperties.PUBLISHED_DATE.setProperty(pageVertexBuilder, revisionTimestamp, visibility);
        }
        RawLumifyProperties.TEXT.setProperty(pageVertexBuilder, textPropertyValue, visibility);
        Vertex pageVertex = pageVertexBuilder.save(authorizations);

        // because save above will cause the StreamingPropertyValue to be read we need to reset the position to 0 for search indexing
        rawPropertyValue.getInputStream().reset();
        textPropertyValue.getInputStream().reset();

        for (LinkWithOffsets link : getLinks(textConverter)) {
            String linkTarget = link.getLinkTargetWithoutHash();
            String linkVertexId = ImportMR.getWikipediaPageVertexId(linkTarget);
            VertexBuilder linkedPageVertexBuilder = prepareVertex(linkVertexId, visibility);
            OntologyLumifyProperties.CONCEPT_TYPE.setProperty(linkedPageVertexBuilder, WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, visibility);
            RawLumifyProperties.MIME_TYPE.setProperty(linkedPageVertexBuilder, ImportMR.WIKIPEDIA_MIME_TYPE, visibility);
            EntityLumifyProperties.SOURCE.addPropertyValue(linkedPageVertexBuilder, ImportMR.MULTI_VALUE_KEY, ImportMR.WIKIPEDIA_SOURCE, visibility);

            titleMetadata = new HashMap<String, Object>();
            LumifyProperties.CONFIDENCE.setMetadata(titleMetadata, 0.1);
            String linkTargetHash = Base64.encodeBase64String(linkTarget.trim().toLowerCase().getBytes());
            LumifyProperties.TITLE.addPropertyValue(linkedPageVertexBuilder, ImportMR.MULTI_VALUE_KEY + "#" + linkTargetHash, linkTarget, titleMetadata, visibility);

            Vertex linkedPageVertex = linkedPageVertexBuilder.save(authorizations);
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
            Text key = ImportMR.getKey(TermMentionModel.TABLE_NAME, termMention.getRowKey().toString().getBytes());
            Mutation m = AccumuloSession.createMutationFromRow(termMention);
            context.write(key, m);
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
        context.write(ImportMR.getKey(dataTableName.toString(), m.getRow()), m);
    }

    @Override
    protected void saveEdgeMutation(Context context, Text edgesTableName, Mutation m) throws IOException, InterruptedException {
        context.write(ImportMR.getKey(edgesTableName.toString(), m.getRow()), m);
    }

    @Override
    protected void saveVertexMutation(Context context, Text verticesTableName, Mutation m) throws IOException, InterruptedException {
        context.write(ImportMR.getKey(verticesTableName.toString(), m.getRow()), m);
    }
}