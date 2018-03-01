package org.elasticsearch.index.spliter;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.index.spliter.job.Schedulers;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * @author xingtianyu(code4j) Created on 2018-2-28.
 */
public class PauseJobAction extends SpliterAction {

    @Inject
    public PauseJobAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_spliter/{splitername}/pause", this);
    }

    @Override
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        XContentBuilder builder = restContentBuilder();
        Spliter spliter = Doc.get(client,INDEX_TMP,TYPE,spliterName);
        Schedulers.pauseJob(spliter);
        builder.startObject()
                .startObject("execution")
                .field("mode","single")
                .startObject("job")
                .field("status","paused")
                .field("name",spliterName)
                .endObject()
                .endObject()
                .endObject();
        restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }


}
