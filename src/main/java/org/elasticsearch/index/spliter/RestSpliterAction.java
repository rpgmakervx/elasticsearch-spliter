package org.elasticsearch.index.spliter;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * @author xingtianyu(code4j) Created on 2018-2-25.
 */
public class RestSpliterAction extends BaseRestHandler {

    @Inject
    public RestSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_spliter/{splitername}", this);
    }

    @Override
    protected void handleRequest(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        XContentBuilder builder = restContentBuilder(restRequest);
        String splitername = restRequest.hasParam("splitername")?
                restRequest.param("splitername"):"spliter";
        builder.startObject().field("greet","welcome to use spliter:"+splitername);
        restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }

    private XContentBuilder restContentBuilder(RestRequest request) throws IOException {
//        获得contentType，一般用json即可
//        XContentType contentType = XContentType.fromRestContentType(request.header("Content-Type"));
        BytesStreamOutput out = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(
                XContentFactory.xContent(XContentType.JSON), out);
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint();
        }
        return builder;
    }
}
