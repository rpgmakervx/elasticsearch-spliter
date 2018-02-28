package org.elasticsearch.index.spliter;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

/**
 * @author xingtianyu(code4j) Created on 2018-2-28.
 */
public class PauseJobAction extends SpliterAction {

    public PauseJobAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    @Override
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {

    }


}
