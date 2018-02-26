package org.elasticsearch.index.spliter;

import com.google.common.base.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class GetSpliterAction extends BaseRestHandler {

    private static final ESLogger logger = Loggers.getLogger(GetSpliterAction.class);

    private static final String TYPE = "config";
    private static final String INDEX_TMP = ".spliter";

    @Inject
    public GetSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_spliter/{splitername}", this);
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
        GetRequest getRequest = new GetRequest(INDEX_TMP, TYPE, splitername);

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
