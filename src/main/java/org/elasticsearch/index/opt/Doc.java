package org.elasticsearch.index.opt;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

/**
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
public class Doc {

    private static final ESLogger logger = Loggers.getLogger(Doc.class);

    public static void get(Client client, GetRequest getRequest,XContentBuilder builder,RestChannel restChannel){
        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                try {
                    builder.startObject().field("found",getResponse.isExists());
                    if (!getResponse.isExists()){
                        builder.endObject();
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                        return ;
                    }
                    Map<String,Object> sourceMap = getResponse.getSource();
                    builder.startObject("config");
                    for (Map.Entry<String,Object> entry:sourceMap.entrySet()){
                        builder.field(entry.getKey(),entry.getValue());
                    }
                    builder.endObject().endObject();
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
                    logger.error("get spliter fail,",e);
                }
            }
        });
    }

    public static void insert(Client client,IndexRequest request , XContentBuilder builder, RestChannel restChannel){

        client.index(request, new ActionListener<IndexResponse>() {
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

}
