package com.altamiracorp.lumify.wikipedia;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.PropertyName;
import com.altamiracorp.lumify.core.model.ontology.Relationship;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.storm.BaseLumifyBolt;
import com.altamiracorp.securegraph.ElementMutation;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.google.inject.Inject;
import org.jdom2.Document;
import org.jdom2.Text;
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
import org.sweble.wikitext.lazy.parser.InternalLink;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

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
    public static final SimpleDateFormat ISO8601DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private Graph graph;
    private Compiler compiler;
    private SimpleWikiConfiguration config;
    private Visibility visibility;
    private Concept wikipediaPageConcept;
    private Relationship wikipediaPageInternalLinkWikipediaPageRelationship;
    private XPathExpression<Text> textXPath;
    private XPathExpression<Text> titleXPath;
    private XPathExpression<Text> revisionTimestampXPath;
    private boolean flushAfterEachRecord;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        flushAfterEachRecord = Boolean.parseBoolean(stormConf.get(WikipediaConstants.CONFIG_FLUSH).toString());
        LOGGER.info("flushAfterEachRecord: " + flushAfterEachRecord);

        try {
            LOGGER.info("Create sweble compiler");
            config = new SimpleWikiConfiguration("classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml");
            compiler = new Compiler(config);
            visibility = new Visibility("");

            textXPath = XPathFactory.instance().compile(TEXT_XPATH, Filters.text());
            titleXPath = XPathFactory.instance().compile(TITLE_XPATH, Filters.text());
            revisionTimestampXPath = XPathFactory.instance().compile(REVISION_TIMESTAMP_XPATH, Filters.text());

            LOGGER.info("Getting ontology concepts");
            wikipediaPageConcept = ontologyRepository.getConceptByName("wikipediaPage");
            if (wikipediaPageConcept == null) {
                throw new RuntimeException("wikipediaPage concept not found");
            }
            wikipediaPageInternalLinkWikipediaPageRelationship = ontologyRepository.getRelationship("wikipediaPageInternalLinkWikipediaPage");
            if (wikipediaPageInternalLinkWikipediaPageRelationship == null) {
                throw new RuntimeException("wikipediaPageInternalLinkWikipediaPage concept not found");
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

        Vertex pageVertex = graph.getVertex(vertexId, getUser().getAuthorizations());
        if (pageVertex == null) {
            throw new RuntimeException("Could not find vertex: " + vertexId);
        }

        StreamingPropertyValue rawValue = (StreamingPropertyValue) pageVertex.getPropertyValue(PropertyName.RAW.toString());
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
        TextConverter p = new TextConverter(config, 100000);
        String text = (String) p.go(compiledPage.getPage());
        if (text.length() == 0) {
            text = wikitext;
        }

        StreamingPropertyValue textPropertyValue = new StreamingPropertyValue(new ByteArrayInputStream(text.getBytes()), String.class);

        ElementMutation<Vertex> m = pageVertex.prepareMutation();
        if (title != null || title.length() > 0) {
            m.addPropertyValue(TITLE_HIGH_PRIORITY, PropertyName.TITLE.toString(), title, visibility);
        }
        if (revisionTimestamp != null) {
            m.setProperty(PropertyName.PUBLISHED_DATE.toString(), revisionTimestamp, visibility);
        }
        m.setProperty(PropertyName.TEXT.toString(), textPropertyValue, visibility);
        m.save();

        for (InternalLink link : p.getInternalLinks()) {
            String linkVertexId = getWikipediaPageVertexId(link.getTarget());
            Vertex linkedPageVertex = graph.prepareVertex(linkVertexId, visibility, getUser().getAuthorizations())
                    .setProperty(PropertyName.CONCEPT_TYPE.toString(), wikipediaPageConcept.getId(), visibility)
                    .setProperty(PropertyName.MIME_TYPE.toString(), "text/plain", visibility)
                    .setProperty(PropertyName.SOURCE.toString(), "Wikipedia", visibility)
                    .addPropertyValue(TITLE_LOW_PRIORITY, PropertyName.TITLE.toString(), link.getTarget(), visibility)
                    .save();
            graph.addEdge(getWikipediaPageToPageEdgeId(pageVertex, linkedPageVertex), pageVertex, linkedPageVertex, wikipediaPageInternalLinkWikipediaPageRelationship.getId().toString(), visibility, getUser().getAuthorizations());
        }

        if (flushAfterEachRecord) {
            graph.flush();
        }
    }

    private String textToString(Text text) {
        if (text == null) {
            return "";
        }
        return text.getText();
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
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
