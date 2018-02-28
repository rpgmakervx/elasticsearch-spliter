package org.elasticsearch.index.spliter;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
abstract public class SpliterAction extends BaseRestHandler {

    protected static final String TYPE = "config";
    protected static final String INDEX_TMP = ".spliter";
    protected static final String WILDCARD = "*";

    protected String spliterName;

    @Inject
    public SpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    @Override
    protected void handleRequest(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        spliterName = restRequest.hasParam("splitername")?
                restRequest.param("splitername"):null;
        action(restRequest,restChannel,client);
    }

    abstract protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception;

    protected XContentBuilder restContentBuilder() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(
                XContentFactory.xContent(XContentType.JSON), out);
        builder.prettyPrint();
        return builder;
    }
}
