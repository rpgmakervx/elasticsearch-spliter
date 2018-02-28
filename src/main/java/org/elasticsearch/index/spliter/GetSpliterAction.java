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
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class GetSpliterAction extends SpliterAction {

    private static final ESLogger logger = Loggers.getLogger(GetSpliterAction.class);

    @Inject
    public GetSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_spliter/{splitername}", this);
    }

    @Override
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
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
        GetRequest getRequest = new GetRequest(INDEX_TMP, TYPE, spliterName);
        Doc.get(client,getRequest,builder,restChannel);

    }

}
