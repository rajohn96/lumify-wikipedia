package com.altamiracorp.lumify.wikipedia.mapreduce;

import com.altamiracorp.lumify.core.util.LumifyLogger;
import com.altamiracorp.lumify.core.util.LumifyLoggerFactory;
import com.altamiracorp.securegraph.GraphFactory;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.elasticsearch.ElasticSearchSearchIndex;
import com.altamiracorp.securegraph.util.MapUtils;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class ImportMRReducer extends Reducer<Text, MutationOrElasticSearchIndexWritable, Text, Mutation> {
    private static final LumifyLogger LOGGER = LumifyLoggerFactory.getLogger(ImportMRReducer.class);
    public static String MAX_ITEMS_PER_REQUEST = "maxItemsPerElasticSearchRequest";
    public static int DEFAULT_MAX_ITEMS_PER_REQUEST = 100;
    private ElasticSearchSearchIndex searchIndex;
    private int maxItemsPerRequest;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Map configurationMap = ImportMR.toMap(context.getConfiguration());
        AccumuloGraph graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(configurationMap, "graph"));
        maxItemsPerRequest = context.getConfiguration().getInt(MAX_ITEMS_PER_REQUEST, DEFAULT_MAX_ITEMS_PER_REQUEST);
        this.searchIndex = (ElasticSearchSearchIndex) graph.getSearchIndex();
    }

    @Override
    protected void reduce(Text keyText, Iterable<MutationOrElasticSearchIndexWritable> values, Context context) throws IOException, InterruptedException {
        try {
            safeReduce(keyText, values, context);
        } catch (Exception ex) {
            LOGGER.error("failed reduce", ex);
            throw new IOException("Could not reduce", ex);
        }
    }

    private void safeReduce(Text keyText, Iterable<MutationOrElasticSearchIndexWritable> values, Context context) throws IOException, InterruptedException {
        String key = keyText.toString();
        context.setStatus(key);
        int keySplitLocation = key.indexOf(ImportMR.KEY_SPLIT);
        if (keySplitLocation < 0) {
            throw new IOException("Invalid key: " + keyText);
        }
        String tableNameString = key.substring(0, keySplitLocation);
        Text tableName = new Text(tableNameString);
        if (ImportMR.TABLE_NAME_ELASTIC_SEARCH.equals(tableNameString)) {
            writeElasticSearchValues(context, key, values);
        } else {
            writeAccumuloMutations(context, key, tableName, values);
        }
    }

    private void writeAccumuloMutations(Context context, String key, Text tableName, Iterable<MutationOrElasticSearchIndexWritable> values) throws IOException, InterruptedException {
        int totalItemCount = 0;
        for (MutationOrElasticSearchIndexWritable m : values) {
            if (totalItemCount % 1000 == 0) {
                context.setStatus(String.format("%s (count: %d)", key, totalItemCount));
            }
            context.write(tableName, m.getMutation());
            totalItemCount++;
        }
    }

    private void writeElasticSearchValues(Context context, String key, Iterable<MutationOrElasticSearchIndexWritable> values) {
        TransportClient client = this.searchIndex.getClient();
        String indexName = this.searchIndex.getIndexName();

        Iterator<MutationOrElasticSearchIndexWritable> it = values.iterator();
        int totalItemCount = 0;
        int itemsInRequest = 0;
        BulkRequestBuilder bulk = client.prepareBulk();
        while (it.hasNext()) {
            MutationOrElasticSearchIndexWritable m = it.next();
            String id = m.getId();
            String json = m.getJson();
            IndexRequestBuilder indexRequest = client.prepareIndex(indexName, ElasticSearchSearchIndex.ELEMENT_TYPE, id);
            indexRequest.setSource(json);
            bulk.add(indexRequest);
            totalItemCount++;
            itemsInRequest++;

            if (itemsInRequest == maxItemsPerRequest) {
                BulkResponse response = bulk
                        .execute()
                        .actionGet();
                LOGGER.debug("bulk response (items: %d): %s", itemsInRequest, response.toString());
                context.setStatus(String.format("%s (count: %d)", key, totalItemCount));
                bulk = client.prepareBulk();
                itemsInRequest = 0;
            }
        }

        if (itemsInRequest > 0) {
            BulkResponse response = bulk
                    .execute()
                    .actionGet();
            LOGGER.debug("bulk response (items: %d): %s", totalItemCount, response.toString());
            context.setStatus(String.format("%s (count: %d)", key, totalItemCount));
        }
        LOGGER.debug("total items sent in reduce: %d", totalItemCount);
    }
}
