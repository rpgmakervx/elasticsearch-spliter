package org.elasticsearch.index.spliter;

import com.google.common.base.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.index.opt.Index;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class AddSpliterAction extends SpliterAction {

    private static final ESLogger logger = Loggers.getLogger(AddSpliterAction.class);

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
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        createIndex(client,restRequest);
        XContentBuilder builder = restContentBuilder();
        if (Strings.isNullOrEmpty(spliterName)){
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
        IndexRequest request = indexRequest(INDEX_TMP,TYPE,spliterName,body);
        Doc.insert(client,request,builder,restChannel);
    }

    private void createIndex(Client client,RestRequest restRequest) throws IOException {
        if (!Index.exists(client,INDEX_TMP)){
            logger.info("logger init index:\n{}",INDEX_TMP);
            Index.createIndex(client,INDEX_TMP);
            createMapper(client,restRequest);
        }
        logger.info("logger don't need to createIndex index:\n{}",INDEX_TMP);
    }

    private void createMapper(Client client,RestRequest restRequest) throws IOException {
        XContentBuilder builder = restContentBuilder();
        Index.createMapper(client,INDEX_TMP,TYPE,builder);
    }

    private IndexRequest indexRequest(String index, String type, String id, String body){
        IndexRequest request = new IndexRequest();
        request.source(body);
        request.type(type);
        request.id(id);
        request.index(index);
        return request;
    }

}
