package org.elasticsearch.index.spliter;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.index.spliter.job.Schedulers;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.quartz.*;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * 执行索引任务
 * 先新建索引，切换别名，别名不存在则新建别名
 * 之后删除过期的索引。
 * 间隔N分钟后执行force_merge
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
public class ExecuteJobAction extends SpliterAction {

    private static final String ALL_CMD = "_all";

    private static final ESLogger logger = Loggers.getLogger(ExecuteJobAction.class);

    @Inject
    public ExecuteJobAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_spliter/{splitername}/execute", this);
    }

    @Override
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        if (ALL_CMD.equals(spliterName)){
            launchSpliter(client);
        }else{
            launchAllSpliter(client);
        }

    }

    private void launchSpliter(Client client) throws SchedulerException {
        Spliter spliter = Doc.get(client,INDEX_TMP,TYPE,spliterName);
        Schedulers.launchJob(spliter);
    }

    private void launchAllSpliter(Client client) throws SchedulerException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX_TMP);
        request.types(TYPE);
        List<Spliter> spliters = Doc.fetchAll(client,request);
        if (spliters == null){
            return;
        }
        for (Spliter spliter:spliters){
            Schedulers.launchJob(spliter);
        }
    }

}
