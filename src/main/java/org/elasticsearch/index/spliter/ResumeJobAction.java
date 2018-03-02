package org.elasticsearch.index.spliter;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.opt.Doc;
import org.elasticsearch.index.spliter.job.Schedulers;
import org.elasticsearch.rest.*;
import org.quartz.SchedulerException;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * @author xingtianyu(code4j) Created on 2018-3-1.
 */
public class ResumeJobAction extends SpliterAction{

    @Inject
    public ResumeJobAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_spliter/{splitername}/resume", this);
    }

    @Override
    protected void action(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        XContentBuilder builder = restContentBuilder();
        logger.info("spliter[{}] job[{}] resuming...",spliterName,spliterName);
        if (ALL_CMD.equals(spliterName)){
            List<String> spliterNames = resumeAllSpliter(client);
            builder.startObject()
                    .startObject("execution")
                    .field("mode","all")
                    .startArray("jobs");
            if (spliterNames != null){
                for (String spliterName:spliterNames){
                    builder.field("name",spliterName)
                            .field("status","resumed");
                }
                builder.endArray().endObject().endObject();
            }
        }else {
            logger.info("spliter[{}] job[{}] resuming...",spliterName,ALL_CMD);
            resumeSpliter(client);
            builder.startObject()
                    .startObject("execution")
                    .field("mode","single")
                    .startObject("job")
                    .field("status","resumed")
                    .field("name",spliterName)
                    .endObject()
                    .endObject()
                    .endObject();
        }
        restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }

    public String resumeSpliter(Client client) throws SchedulerException {
        Spliter spliter = Doc.get(client,INDEX_TMP,TYPE,spliterName);
        Schedulers.resumeJob(spliter);
        return spliter.getSpliterName();
    }

    public List<String> resumeAllSpliter(Client client) throws SchedulerException {
        SearchRequest request = new SearchRequest();
        request.indices(INDEX_TMP);
        request.types(TYPE);
        List<Spliter> spliters = Doc.fetchAll(client,request);
        if (spliters == null){
            return null;
        }
        List<String> spliterNames = new ArrayList<>();
        for (Spliter spliter:spliters){
            Schedulers.resumeJob(spliter);
            spliterNames.add(spliter.getSpliterName());
        }
        return spliterNames;
    }
}
