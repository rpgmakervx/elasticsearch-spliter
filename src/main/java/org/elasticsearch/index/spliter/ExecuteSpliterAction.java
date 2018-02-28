package org.elasticsearch.index.spliter;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperUtils;
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.index.opt.Index;
import org.elasticsearch.index.spliter.job.SpliterJob;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.joda.time.DateTime;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * 执行索引任务
 * 先新建索引，切换别名，别名不存在则新建别名
 * 之后删除过期的索引。
 * 间隔N分钟后执行force_merge
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
public class ExecuteSpliterAction extends SpliterAction {

    private static final String ALL_CMD = "_all";

    private static final ESLogger logger = Loggers.getLogger(ExecuteSpliterAction.class);

    @Inject
    public ExecuteSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_spliter/{splitername}/execute", this);
    }

    @Override
    protected void handleRequest(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        super.handleRequest(restRequest,restChannel,client);

    }

    private void launchSpliter(Client client){

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
            SchedulerFactory factory = new StdSchedulerFactory();
            Scheduler scheduler = factory.getScheduler();
            JobDetail detail = JobBuilder.newJob(SpliterJob.class)
                    .withDescription(spliter.getDesc())
                    .withIdentity(spliter.getSpliterName(),spliter.getSpliterName())
                    .build();
            Trigger trigger = TriggerBuilder.newTrigger()
                    .startNow()
                    .withIdentity(spliter.getSpliterName(),spliter.getSpliterName())
                    .withSchedule(CronScheduleBuilder.cronSchedule(spliter.getPeroid()))
                    .build();
            scheduler.scheduleJob(detail,trigger);
            scheduler.start();
        }

    }

}
