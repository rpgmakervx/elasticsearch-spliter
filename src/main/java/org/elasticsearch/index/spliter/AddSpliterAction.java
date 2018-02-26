package org.elasticsearch.index.spliter;

import com.google.common.base.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class AddSpliterAction extends BaseRestHandler {

    private static final String TYPE = "config";
    private static final String INDEX_TMP = ".spliter";

    protected AddSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(PUT, "/_spliter/{splitername}", this);
        controller.registerHandler(POST, "/_spliter/{splitername}", this);
    }

    @Override
    protected void handleRequest(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
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
