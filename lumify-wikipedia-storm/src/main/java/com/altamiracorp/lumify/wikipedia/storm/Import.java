package com.altamiracorp.lumify.wikipedia.storm;

import com.altamiracorp.bigtable.model.FlushFlag;
import com.altamiracorp.lumify.core.cmdline.CommandLineBase;
import com.altamiracorp.lumify.core.model.audit.AuditAction;
import com.altamiracorp.lumify.core.model.audit.AuditRepository;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.OntologyRepository;
import com.altamiracorp.lumify.core.model.workQueue.WorkQueueRepository;
import com.altamiracorp.lumify.core.security.LumifyVisibility;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.lumify.wikipedia.RandomAccessFileInputStream;
import com.altamiracorp.lumify.wikipedia.WikipediaConstants;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.VertexBuilder;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.google.inject.Inject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.json.JSONObject;

import java.io.*;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.altamiracorp.lumify.core.model.ontology.OntologyLumifyProperties.CONCEPT_TYPE;
import static com.altamiracorp.lumify.core.model.properties.EntityLumifyProperties.SOURCE;
import static com.altamiracorp.lumify.core.model.properties.LumifyProperties.TITLE;
import static com.altamiracorp.lumify.core.model.properties.RawLumifyProperties.MIME_TYPE;
import static com.altamiracorp.lumify.core.model.properties.RawLumifyProperties.RAW;

public class Import extends CommandLineBase {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(Import.class);
    private static final DecimalFormat numberFormatter = new DecimalFormat("#,###");
    private static final Pattern pageTitlePattern = Pattern.compile(".*?<title>(.*?)</title>.*");
    private static final String AUDIT_PROCESS_NAME = Import.class.getName();

    private Graph graph;
    private WorkQueueRepository workQueueRepository;
    private OntologyRepository ontologyRepository;
    private AuditRepository auditRepository;
    private final LumifyVisibility lumifyVisibility = new LumifyVisibility();
    private long startLine = 0;
    private Long startOffset = null;
    private int pageCountToImport = Integer.MAX_VALUE;
    private boolean flush;
    private Concept wikipediaPageConcept;
    private RandomAccessFile randomAccessFile = null;
    private InputStream in;
    private String wikipediaPageConceptId;

    public static void main(String[] args) throws Exception {
        int res = new Import().run(args);
        if (res != 0) {
            System.exit(res);
        }
    }

    @Override
    protected Options getOptions() {
        Options options = super.getOptions();

        options.addOption(
                OptionBuilder
                        .withLongOpt("in")
                        .withDescription("Input file name")
                        .hasArg(true)
                        .withArgName("file")
                        .create("i")
        );

        options.addOption(
                OptionBuilder
                        .withLongOpt("pagecount")
                        .withDescription("Number of pages to import. (default: all)")
                        .hasArg(true)
                        .withArgName("number")
                        .create()
        );

        options.addOption(
                OptionBuilder
                        .withLongOpt("startline")
                        .withDescription("The line number to start at.")
                        .hasArg(true)
                        .withArgName("number")
                        .create()
        );

        options.addOption(
                OptionBuilder
                        .withLongOpt("startoffset")
                        .withDescription("The byte offset to start at.")
                        .hasArg(true)
                        .withArgName("number")
                        .create()
        );

        options.addOption(
                OptionBuilder
                        .withLongOpt("flush")
                        .withDescription("Flush after each page")
                        .hasArg(false)
                        .create()
        );

        return options;
    }

    @Override
    protected void processOptions(CommandLine cmd) throws Exception {
        super.processOptions(cmd);

        if (cmd.hasOption("startline")) {
            startLine = Long.parseLong(cmd.getOptionValue("startline"));
        }

        if (cmd.hasOption("startoffset")) {
            startOffset = Long.parseLong(cmd.getOptionValue("startoffset"));
        }

        if (cmd.hasOption("pagecount")) {
            pageCountToImport = Integer.parseInt(cmd.getOptionValue("pagecount"));
        }

        flush = cmd.hasOption("flush");
        String inputFileName;
        inputFileName = cmd.getOptionValue("in");
        if (inputFileName == null) {
            throw new RuntimeException("in is required");
        }
        LOGGER.info("Loading " + inputFileName);
        File inputFile = new File(inputFileName);
        if (!inputFile.exists()) {
            throw new RuntimeException("Could not find " + inputFileName);
        }

        if (inputFile.getName().endsWith("bz2")) {
            if (startOffset != null) {
                throw new RuntimeException("start offset not supported for bz2 files");
            }
            FileInputStream fileInputStream = new FileInputStream(inputFile);
            in = new BZip2CompressorInputStream(fileInputStream);
        } else {
            randomAccessFile = new RandomAccessFile(inputFile, "r");
            if (startOffset != null) {
                randomAccessFile.seek(startOffset);
            }
            in = new RandomAccessFileInputStream(randomAccessFile);
        }
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        wikipediaPageConcept = ontologyRepository.getConceptById("wikipediaPage");
        if (wikipediaPageConcept == null) {
            throw new RuntimeException("wikipediaPage concept not found");
        }

        wikipediaPageConceptId = wikipediaPageConcept.getId();

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        try {
            long lineNumber = 1;
            int pageCount = 0;
            String line;
            StringBuilder page = null;
            Matcher m;
            String pageTitle = null;

            if (startLine > 0) {
                while (reader.readLine() != null && lineNumber < startLine) {
                    if ((lineNumber % 100000) == 0) {
                        LOGGER.info("Skipping line " + numberFormatter.format(lineNumber));
                    }
                    lineNumber++;
                }
            }

            while ((line = reader.readLine()) != null) {
                if ((lineNumber % 100000) == 0) {
                    LOGGER.info("Processing line " + numberFormatter.format(lineNumber) + (randomAccessFile == null ? "" : " (offset: " + randomAccessFile.getFilePointer() + ")"));
                }
                if (page != null) {
                    page.append(line);
                    page.append("\n");
                }
                if (line.contains("<page>") && line.trim().equals("<page>")) {
                    page = new StringBuilder();
                    pageTitle = null;
                    page.append(line);
                    page.append("\n");
                } else if (line.contains("<title>") && (m = pageTitlePattern.matcher(line)) != null && m.matches()) {
                    pageTitle = m.group(1);
                } else if (page != null && line.contains("</page>") && line.trim().equals("</page>")) {
                    pageCount++;
                    if ((pageCount % 1000) == 0) {
                        LOGGER.info("Processing page " + numberFormatter.format(pageCount));
                    }

                    if (pageTitle == null) {
                        LOGGER.error("Found end page without page title. Line %d", lineNumber);
                    } else {
                        String pageString = page.toString();
                        page = null;
                        Vertex wikipediaPageVertex = savePageVertex(pageString, pageTitle, wikipediaPageConcept);
                        if (flush || pageCount < 100) { // We call flush for the first 100 so that we can saturate the storm topology otherwise we'll get vertex not found problems.
                            this.graph.flush();
                        }
                        JSONObject workJson = new JSONObject();
                        workJson.put("vertexId", wikipediaPageVertex.getId().toString());
                        this.workQueueRepository.pushOnQueue(WikipediaConstants.WIKIPEDIA_QUEUE, FlushFlag.NO_FLUSH, workJson);
                    }

                    if (pageCount >= pageCountToImport) {
                        break;
                    }
                }
                lineNumber++;
            }
        } finally {
            this.graph.flush();
            this.workQueueRepository.flush();
            reader.close();
        }

        return 0;
    }

    private Vertex savePageVertex(String pageString, String pageTitle, Concept wikipediaPageConcept) {
        String wikipediaPageVertexId = WikipediaBolt.getWikipediaPageVertexId(pageTitle);
        StreamingPropertyValue rawPropertyValue = new StreamingPropertyValue(new ByteArrayInputStream(pageString.getBytes()), byte[].class);
        rawPropertyValue.store(true);
        rawPropertyValue.searchIndex(false);
        VertexBuilder builder = graph.prepareVertex(wikipediaPageVertexId, lumifyVisibility.getVisibility(), getAuthorizations());
        CONCEPT_TYPE.setProperty(builder, wikipediaPageConceptId, lumifyVisibility.getVisibility());
        RAW.setProperty(builder, rawPropertyValue, lumifyVisibility.getVisibility());
        TITLE.addPropertyValue(builder, WikipediaBolt.TITLE_MEDIUM_PRIORITY, pageTitle, lumifyVisibility.getVisibility());
        MIME_TYPE.setProperty(builder, WikipediaBolt.WIKIPEDIA_MIME_TYPE, lumifyVisibility.getVisibility());
        SOURCE.setProperty(builder, WikipediaBolt.WIKIPEDIA_SOURCE, lumifyVisibility.getVisibility());
        Vertex vertex = builder.save();

        this.auditRepository.auditVertex(AuditAction.UPDATE, vertex.getId(), AUDIT_PROCESS_NAME, "Raw set", getUser(), FlushFlag.NO_FLUSH, new Visibility(""));

        return vertex;
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public void setWorkQueueRepository(WorkQueueRepository workQueueRepository) {
        this.workQueueRepository = workQueueRepository;
    }

    @Inject
    public void setOntologyRepository(OntologyRepository ontologyRepository) {
        this.ontologyRepository = ontologyRepository;
    }

    @Inject
    public void setAuditRepository(AuditRepository auditRepository) {
        this.auditRepository = auditRepository;
    }
}
