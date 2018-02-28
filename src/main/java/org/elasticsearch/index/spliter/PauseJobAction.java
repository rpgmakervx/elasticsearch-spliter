package org.elasticsearch.index.spliter;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.index.spliter.job.Schedulers;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * @author xingtianyu(code4j) Created on 2018-2-28.
 */
public class PauseJobAction extends SpliterAction {

    public PauseJobAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_spliter/{splitername}/pause", this);
    }

    @Override
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        Spliter spliter = Doc.get(client,INDEX_TMP,TYPE,spliterName);
        Schedulers.pauseJob(spliter);
    }


}
