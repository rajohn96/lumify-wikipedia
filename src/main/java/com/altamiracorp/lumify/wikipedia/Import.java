package com.altamiracorp.lumify.wikipedia;

import com.altamiracorp.bigtable.model.FlushFlag;
import com.altamiracorp.lumify.core.cmdline.CommandLineBase;
import com.altamiracorp.lumify.core.model.ontology.Concept;
import com.altamiracorp.lumify.core.model.ontology.OntologyRepository;
import com.altamiracorp.lumify.core.model.ontology.PropertyName;
import com.altamiracorp.lumify.core.model.workQueue.WorkQueueRepository;
import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.securegraph.Graph;
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

public class Import extends CommandLineBase {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(Import.class);
    private static final DecimalFormat numberFormatter = new DecimalFormat("#,###");
    private static final Pattern pageTitlePattern = Pattern.compile(".*?<title>(.*?)</title>.*");
    private Graph graph;
    private WorkQueueRepository workQueueRepository;
    private OntologyRepository ontologyRepository;

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
                        .withLongOpt("flush")
                        .withDescription("Flush after each page")
                        .hasArg(false)
                        .create()
        );

        return options;
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        Visibility visibility = new Visibility("");

        int pageCountToImport = Integer.MAX_VALUE;
        if (cmd.hasOption("pagecount")) {
            pageCountToImport = Integer.parseInt(cmd.getOptionValue("pagecount"));
        }

        boolean flush = cmd.hasOption("flush");
        String inputFileName = cmd.getOptionValue("in");
        if (inputFileName == null) {
            throw new RuntimeException("in is required");
        }
        LOGGER.info("Loading " + inputFileName);
        File inputFile = new File(inputFileName);
        if (!inputFile.exists()) {
            throw new RuntimeException("Could not find " + inputFileName);
        }

        Concept wikipediaPageConcept = ontologyRepository.getConceptByName("wikipediaPage");
        if (wikipediaPageConcept == null) {
            throw new RuntimeException("wikipediaPage concept not found");
        }

        FileInputStream fileInputStream = new FileInputStream(inputFile);
        BZip2CompressorInputStream in = new BZip2CompressorInputStream(fileInputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        try {
            int lineNumber = 1;
            int pageCount = 0;
            String line;
            StringBuilder page = null;
            Matcher m;
            String pageTitle = null;
            while ((line = reader.readLine()) != null) {
                if ((lineNumber % 100000) == 0) {
                    LOGGER.info("Processing line " + numberFormatter.format(lineNumber));
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
                } else if ((m = pageTitlePattern.matcher(line)) != null && m.matches()) {
                    pageTitle = m.group(1);
                } else if (line.contains("</page>") && line.trim().equals("</page>")) {
                    pageCount++;
                    if ((pageCount % 1000) == 0) {
                        LOGGER.info("Processing page " + numberFormatter.format(pageCount));
                    }

                    if (page == null) {
                        LOGGER.error("Found end page without start page. Line %d", lineNumber);
                    } else if (pageTitle == null) {
                        LOGGER.error("Found end page without page title. Line %d", lineNumber);
                    } else {
                        String pageString = page.toString();
                        String wikipediaPageVertexId = WikipediaBolt.getWikipediaPageVertexId(pageTitle);
                        StreamingPropertyValue rawPropertyValue = new StreamingPropertyValue(new ByteArrayInputStream(pageString.getBytes()), byte[].class);
                        rawPropertyValue.store(true);
                        rawPropertyValue.searchIndex(false);
                        graph.prepareVertex(wikipediaPageVertexId, visibility, getUser().getAuthorizations())
                                .setProperty(PropertyName.CONCEPT_TYPE.toString(), wikipediaPageConcept.getId(), visibility)
                                .setProperty(PropertyName.RAW.toString(), rawPropertyValue, visibility)
                                .addPropertyValue(WikipediaBolt.TITLE_MEDIUM_PRIORITY, PropertyName.TITLE.toString(), pageTitle, visibility)
                                .setProperty(PropertyName.MIME_TYPE.toString(), "text/plain", visibility)
                                .setProperty(PropertyName.SOURCE.toString(), "Wikipedia", visibility)
                                .save();
                        if (flush || pageCount < 100) { // We call flush for the first 100 so that we can saturate the storm topology otherwise we'll get vertex not found problems.
                            graph.flush();
                        }
                        JSONObject workJson = new JSONObject();
                        workJson.put("vertexId", wikipediaPageVertexId);
                        this.workQueueRepository.pushOnQueue(WikipediaConstants.WIKIPEDIA_QUEUE, FlushFlag.NO_FLUSH, workJson);
                    }

                    if (pageCount >= pageCountToImport) {
                        break;
                    }
                }
                lineNumber++;
            }
        } finally {
            graph.flush();
            this.workQueueRepository.flush();
            reader.close();
        }

        return 0;
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
}
