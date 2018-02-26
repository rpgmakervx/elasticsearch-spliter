package org.elasticsearch.index.spliter;

import com.google.common.base.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class AddSpliterAction extends BaseRestHandler {

    private static final String TYPE = "config";
    private static final String INDEX_TMP = ".spliter";

    @Inject
    public AddSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(PUT, "/_spliter/{splitername}", this);
        controller.registerHandler(POST, "/_spliter/{splitername}", this);
    }

    /**
     * 若不存在.spliter索引，先创建一个
     * @param restRequest
     * @param restChannel
     * @param client
     * @throws Exception
     */
    @Override
    protected void handleRequest(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        createIndex(client,restRequest);
        XContentBuilder builder = restContentBuilder(restRequest);
        String splitername = restRequest.hasParam("splitername")?
                restRequest.param("splitername"):null;
        if (Strings.isNullOrEmpty(splitername)){
            builder.startObject()
                    .startObject("error")
                    .field("status",400)
                    .field("reason","spliter name could not be null")
                    .endObject()
                    .endObject();
            restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            return;
        }
        String body = restRequest.content().toUtf8();
        client.index(indexRequest(splitername,body), new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                try {
                    if (indexResponse.isCreated()){
                        builder.startObject().field("operate","created").field("status",200);
                    }else{
                        builder.startObject().field("operate","cover").field("status",304);
                    }
                    builder.endObject();
                    restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void onFailure(Throwable throwable) {
                try {
                    restChannel.sendResponse(
                            new BytesRestResponse(restChannel, RestStatus.OK, throwable));
                } catch (IOException e) {
                    logger.error("build spliter fail,",e);
                }
            }
        });

    }

    private IndexRequest indexRequest(String id,String body){
        IndexRequest request = new IndexRequest();
        request.source(body);
        request.type(TYPE);
        request.id(id);
        request.index(INDEX_TMP);
        return request;
    }

    private void createIndex(Client client,RestRequest restRequest) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(INDEX_TMP);
        GetIndexResponse getIndexResponse = client.admin().indices().getIndex(getIndexRequest).actionGet();
        String[] indices = getIndexResponse.getIndices();
        if (indices == null||indices.length == 0){
            CreateIndexRequest request = new CreateIndexRequest();
            request.index(INDEX_TMP);
            CreateIndexResponse response = client.admin().indices().create(request).actionGet();
            response.isAcknowledged();
            createMapper(client,restRequest);
        }
    }

    private void createMapper(Client client,RestRequest restRequest) throws IOException {
        XContentBuilder builder = restContentBuilder(restRequest);
        builder.startObject()
                .startObject(TYPE)
                .startObject(SpliterConstant.PROPERTIES);
        for(Map.Entry<String,Map<String,String>> entry:SpliterIndexMapper.mapper.entrySet()){
            builder.startObject(entry.getKey());
            for (Map.Entry<String,String> metaEntry:entry.getValue().entrySet()){
                builder.field(metaEntry.getKey(),metaEntry.getValue());
            }
            builder.endObject();
        }
        builder.endObject().endObject().endObject();
        PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_TMP).type(TYPE).source(builder);
        client.admin().indices().putMapping(mappingRequest).actionGet();
    }

    private XContentBuilder restContentBuilder(RestRequest request) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(
                XContentFactory.xContent(XContentType.JSON), out);
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint();
        }
        return builder;
    }
}
