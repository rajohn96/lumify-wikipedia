package com.altamiracorp.lumify.wikipedia.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.altamiracorp.bigtable.model.FlushFlag;
import com.altamiracorp.lumify.core.model.audit.AuditAction;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.Relationship;
import com.altamiracorp.lumify.core.model.termMention.TermMentionModel;
import com.altamiracorp.lumify.core.model.termMention.TermMentionRepository;
import com.altamiracorp.lumify.core.model.termMention.TermMentionRowKey;
import com.altamiracorp.lumify.core.security.LumifyVisibility;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.storm.BaseLumifyBolt;
import com.altamiracorp.lumify.wikipedia.InternalLinkWithOffsets;
import com.altamiracorp.lumify.wikipedia.TextConverter;
import com.altamiracorp.lumify.wikipedia.WikipediaConstants;
import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.mutation.ElementMutation;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.google.inject.Inject;
import org.jdom2.Document;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.json.JSONObject;
import org.sweble.wikitext.engine.CompiledPage;
import org.sweble.wikitext.engine.Compiler;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.altamiracorp.lumify.core.model.ontology.OntologyLumifyProperties.CONCEPT_TYPE;
import static com.altamiracorp.lumify.core.model.properties.EntityLumifyProperties.SOURCE;
import static com.altamiracorp.lumify.core.model.properties.LumifyProperties.TITLE;
import static com.altamiracorp.lumify.core.model.properties.RawLumifyProperties.*;

/*

Example page xml

<page>
    <title>AccessibleComputing</title>
    <ns>0</ns>
    <id>10</id>
    <redirect title="Computer accessibility" />
    <revision>
      <id>381202555</id>
      <parentid>381200179</parentid>
      <timestamp>2010-08-26T22:38:36Z</timestamp>
      <contributor>
        <username>OlEnglish</username>
        <id>7181920</id>
      </contributor>
      <minor />
      <comment>[[Help:Reverting|Reverted]] edits by [[Special:Contributions/76.28.186.133|76.28.186.133]] ([[User talk:76.28.186.133|talk]]) to last version by Gurch</comment>
      <text xml:space="preserve">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text>
      <sha1>lo15ponaybcg2sf49sstw9gdjmdetnk</sha1>
      <model>wikitext</model>
      <format>text/x-wiki</format>
    </revision>
  </page>
 */

public class WikipediaBolt extends BaseLumifyBolt {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(WikipediaBolt.class);
    public static final String WIKIPEDIA_ID_PREFIX = "WIKIPEDIA_";
    public static final String WIKIPEDIA_LINK_ID_PREFIX = "WIKIPEDIA_LINK_";
    public static final String TITLE_HIGH_PRIORITY = "0";
    public static final String TITLE_MEDIUM_PRIORITY = "1";
    public static final String TITLE_LOW_PRIORITY = "2";
    public static final String TEXT_XPATH = "/page/revision/text/text()";
    public static final String TITLE_XPATH = "/page/title/text()";
    public static final String REVISION_TIMESTAMP_XPATH = "/page/revision/timestamp/text()";
    public static final String WIKIPEDIA_MIME_TYPE = "text/plain";
    public static final String WIKIPEDIA_SOURCE = "Wikipedia";
    public static final SimpleDateFormat ISO8601DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final String AUDIT_PROCESS_NAME = WikipediaBolt.class.getName();

    private Graph graph;
    private TermMentionRepository termMentionRepository;
    private Compiler compiler;
    private SimpleWikiConfiguration config;
    private LumifyVisibility lumifyVisibility;
    private Relationship wikipediaPageInternalLinkWikipediaPageRelationship;
    private XPathExpression<org.jdom2.Text> textXPath;
    private XPathExpression<org.jdom2.Text> titleXPath;
    private XPathExpression<org.jdom2.Text> revisionTimestampXPath;
    private boolean flushAfterEachRecord;
    private String wikipediaPageConceptId;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        flushAfterEachRecord = Boolean.parseBoolean(stormConf.get(WikipediaConstants.CONFIG_FLUSH).toString());
        LOGGER.info("flushAfterEachRecord: " + flushAfterEachRecord);

        try {
            LOGGER.info("Create sweble compiler");
            config = new SimpleWikiConfiguration("classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml");
            compiler = new Compiler(config);
            lumifyVisibility = new LumifyVisibility();

            textXPath = XPathFactory.instance().compile(TEXT_XPATH, Filters.text());
            titleXPath = XPathFactory.instance().compile(TITLE_XPATH, Filters.text());
            revisionTimestampXPath = XPathFactory.instance().compile(REVISION_TIMESTAMP_XPATH, Filters.text());

            LOGGER.info("Getting ontology concepts");
            Concept wikipediaPageConcept = ontologyRepository.getConceptById(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI);
            if (wikipediaPageConcept == null) {
                throw new RuntimeException(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI + " concept not found");
            }
            wikipediaPageConceptId = wikipediaPageConcept.getId();

            wikipediaPageInternalLinkWikipediaPageRelationship = ontologyRepository.getRelationshipById(WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI);
            if (wikipediaPageInternalLinkWikipediaPageRelationship == null) {
                throw new RuntimeException(WikipediaConstants.WIKIPEDIA_PAGE_INTERNAL_LINK_WIKIPEDIA_PAGE_CONCEPT_URI + " concept not found");
            }

            LOGGER.info("prepare complete");
        } catch (Exception e) {
            collector.reportError(e);
            throw new RuntimeException("Could not initialize", e);
        }
    }

    @Override
    public void cleanup() {
        graph.flush();
        super.cleanup();
    }

    @Override
    protected void safeExecute(Tuple input) throws Exception {
        JSONObject json = getJsonFromTuple(input);
        String vertexId = json.getString("vertexId");
        LOGGER.info("processing wikipedia page: " + vertexId);

        Vertex pageVertex = graph.getVertex(vertexId, getAuthorizations());
        if (pageVertex == null) {
            throw new RuntimeException("Could not find vertex: " + vertexId);
        }

        StreamingPropertyValue rawValue = RAW.getPropertyValue(pageVertex);
        if (rawValue == null) {
            throw new RuntimeException("Could not get raw value from vertex: " + vertexId);
        }

        InputStream in = rawValue.getInputStream();
        String wikitext;
        String title;
        Date revisionTimestamp = null;
        try {
            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(in);
            title = textToString(titleXPath.evaluateFirst(doc));
            wikitext = textToString(textXPath.evaluateFirst(doc));
            String revisionTimestampString = textToString(revisionTimestampXPath.evaluateFirst(doc));
            try {
                revisionTimestamp = ISO8601DATEFORMAT.parse(revisionTimestampString);
            } catch (Exception ex) {
                LOGGER.error("Could not parse revision timestamp %s", revisionTimestampString, ex);
            }
        } finally {
            in.close();
        }

        String fileTitle = vertexId;
        PageTitle pageTitle = PageTitle.make(config, fileTitle);
        PageId pageId = new PageId(pageTitle, -1);
        CompiledPage compiledPage = compiler.postprocess(pageId, wikitext, null);
        TextConverter p = new TextConverter(config);
        String text = (String) p.go(compiledPage.getPage());
        if (text.length() == 0) {
            text = wikitext;
        }

        StreamingPropertyValue textPropertyValue = new StreamingPropertyValue(new ByteArrayInputStream(text.getBytes()), String.class);

        ElementMutation<Vertex> m = pageVertex.prepareMutation();
        if (title != null && !title.trim().isEmpty()) {
            TITLE.addPropertyValue(m, TITLE_HIGH_PRIORITY, title, lumifyVisibility.getVisibility());
        }
        if (revisionTimestamp != null) {
            PUBLISHED_DATE.setProperty(m, revisionTimestamp, lumifyVisibility.getVisibility());
        }
        TEXT.setProperty(m, textPropertyValue, lumifyVisibility.getVisibility());
        m.save();

        this.auditRepository.auditVertex(AuditAction.UPDATE, pageVertex.getId(), AUDIT_PROCESS_NAME, "Page processed", getUser(), FlushFlag.NO_FLUSH, lumifyVisibility.getVisibility());

        for (InternalLinkWithOffsets link : p.getInternalLinks()) {
            String linkVertexId = getWikipediaPageVertexId(link.getLink().getTarget());
            VertexBuilder builder = graph.prepareVertex(linkVertexId, lumifyVisibility.getVisibility(), getAuthorizations());
            CONCEPT_TYPE.setProperty(builder, wikipediaPageConceptId, lumifyVisibility.getVisibility());
            MIME_TYPE.setProperty(builder, WIKIPEDIA_MIME_TYPE, lumifyVisibility.getVisibility());
            SOURCE.setProperty(builder, WIKIPEDIA_SOURCE, lumifyVisibility.getVisibility());
            TITLE.addPropertyValue(builder, TITLE_LOW_PRIORITY, link.getLink().getTarget(), lumifyVisibility.getVisibility());
            Vertex linkedPageVertex = builder.save();
            Edge edge = graph.addEdge(getWikipediaPageToPageEdgeId(pageVertex, linkedPageVertex), pageVertex, linkedPageVertex,
                    wikipediaPageInternalLinkWikipediaPageRelationship.getId(), lumifyVisibility.getVisibility(), getAuthorizations());
            auditRepository.auditRelationship(AuditAction.CREATE, pageVertex, linkedPageVertex,
                    edge, AUDIT_PROCESS_NAME, "internal link created",
                    getUser(), new Visibility(""));

            TermMentionModel termMention = new TermMentionModel(new TermMentionRowKey(pageVertex.getId().toString(), link.getStartOffset(),
                    link.getEndOffset()));
            termMention.getMetadata()
                    .setConceptGraphVertexId(wikipediaPageConceptId, lumifyVisibility.getVisibility())
                    .setSign(link.getLink().getTarget(), lumifyVisibility.getVisibility())
                    .setVertexId(linkedPageVertex.getId().toString(), lumifyVisibility.getVisibility())
                    .setOntologyClassUri(WikipediaConstants.WIKIPEDIA_PAGE_CONCEPT_URI, lumifyVisibility.getVisibility());
            this.termMentionRepository.save(termMention, FlushFlag.NO_FLUSH);
        }

        if (flushAfterEachRecord) {
            this.termMentionRepository.flush();
            this.graph.flush();
        }
    }

    private String textToString(org.jdom2.Text text) {
        if (text == null) {
            return "";
        }
        return text.getText();
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public void setTermMentionRepository(TermMentionRepository termMentionRepository) {
        this.termMentionRepository = termMentionRepository;
    }

    private static String getWikipediaPageToPageEdgeId(Vertex pageVertex, Vertex linkedPageVertex) {
        return WIKIPEDIA_LINK_ID_PREFIX + getWikipediaPageTitleFromId(pageVertex.getId()) + "_" + getWikipediaPageTitleFromId(linkedPageVertex.getId());
    }

    private static String getWikipediaPageTitleFromId(Object id) {
        return id.toString().substring(WIKIPEDIA_ID_PREFIX.length());
    }

    public static String getWikipediaPageVertexId(String pageTitle) {
        return WIKIPEDIA_ID_PREFIX + pageTitle;
    }
}
