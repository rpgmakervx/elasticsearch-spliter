package org.elasticsearch.index.spliter.job;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.opt.Index;
import org.elasticsearch.index.spliter.Spliter;
import org.elasticsearch.index.spliter.TimeKits;
import org.joda.time.DateTime;
import org.quartz.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class SpliterJob implements Job{

    private static final ESLogger logger = Loggers.getLogger(SpliterJob.class);
    protected static final String TYPE = "config";
    protected static final String INDEX_TMP = ".spliter";
    protected static final String WILDCARD = "*";

    private boolean terminate = false;

    private boolean inited = false;

    public static JobKey jobKey;

    private String spliterName;

    private Client client;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            System.out.println("execute job");
            logger.info("spliter[{}] execute.",spliterName);
            init(context.getJobDetail());
            jobKey = context.getJobDetail().getKey();
            doJob();
        }catch (Exception e){
            logger.error("spliter[{}] Exception occur while job is running.",e);
        }
    }

    private void init(JobDetail job){
        if (!inited){
            spliterName = job.getJobDataMap().getString("spliterName");
            client = (Client) job.getJobDataMap().get("client");
            inited = true;
        }
    }

    private void doJob(){
        GetRequest getRequest = new GetRequest(INDEX_TMP, TYPE, spliterName);
        GetResponse response = client.get(getRequest).actionGet();
        Map<String,Object> sourceMap = response.getSource();
        Spliter spliter = Spliter.Builder.build(sourceMap);
        if(createIndex(client,spliter)){
            try {
                if (bindAlias(client,spliter)){
                    deleteIndex(client,spliter);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void deleteIndex(Client client, Spliter spliter){
        List<String> indices = Index.getIndices(client,spliter.getIndexName()+WILDCARD);
        if (indices == null){
            return;
        }
        for (String index:indices){
            String createTime = index.replace(spliter.getIndexName(),"");
            DateTime createDateTime = TimeKits.getDateTime(createTime,spliter.getFormat());
            DateTime invalidDateTime = createDateTime.plus(TimeUnit.SECONDS.toMillis(spliter.getRemain()));
            //true说明超时，当前索引需要删除
            if (TimeKits.after(DateTime.now(),invalidDateTime)){
                logger.info("spliter[{}] delete index name:{}, invalidDateTime:{},format:{}",spliterName,index,invalidDateTime,spliter.getFormat());
                Index.deleteIndex(client,index);
            }
        }

    }

    private boolean createIndex(Client client,Spliter spliter){
        String dateTime = TimeKits.getCurrentDateFormatted(spliter.getFormat());
        String indexName = spliter.getIndexName() + dateTime;
        logger.info("spliter[{}] create new index[{}]",spliter.getSpliterName(),indexName);
        boolean created = Index.createIndex(client,indexName);
        if (!created){
            logger.warn("spliter[{}] create index fail,indexName [{}]",spliterName,indexName);
            return false;
        }
        return true;
    }

    private boolean bindAlias(Client client,Spliter spliter) throws IOException {
        XContentBuilder builder = restContentBuilder();
        if (Strings.isNullOrEmpty(spliter.getAliaName())){
            logger.error("spliter[{}] dose not have alia name",spliterName);
            return false;
        }
        String dateTime = TimeKits.getCurrentDateFormatted(spliter.getFormat());
        String indexName = spliter.getIndexName() + dateTime;
        boolean aliaExists = Index.exists(client,spliter.getAliaName());
        if (!aliaExists){
            builder.startObject()
                    .startObject("actions")
                    .startArray()
                    .startObject("add")
                    .field("index",indexName)
                    .field("alias",spliter.getAliaName())
                    .endObject()
                    .endArray()
                    .endObject()
                    .endObject();
            boolean ack = Index.createAlias(client,builder);
            logger.info("spliter[{}] create alias[{}] for index[{}]",spliter.getSpliterName(),spliter.getAliaName(),indexName);
            if (!ack){
                logger.error("spliter[{}] create alia fail,indexName[{}] aliasName[{}]",spliterName,indexName,spliter.getAliaName());
                return false;
            }
            return ack;
        }
        builder.startObject()
                .startObject("actions")
                .startArray()
                .startObject("add")
                .field("index",indexName)
                .field("alias",spliter.getAliaName())
                .endObject();
        List<String> unBindIndices = Index.getIndices(client,spliter.getAliaName());
        for (String unBindIndex:unBindIndices){
            builder.startObject("remove")
                    .field("index",unBindIndex)
                    .field("alias",spliter.getAliaName())
                    .endObject();
        }
        builder.endArray().endObject().endObject();
        logger.info("spliter[{}] bind alias[{}] to index[{}]",spliter.getSpliterName(),spliter.getAliaName(),indexName);
        if (!Index.createAlias(client,builder)){
            logger.error("spliter[{}] rebind alias fail, rest body is:\n{}",spliterName,builder.string());
            return false;
        }
        return true;
    }

    public String getSpliterName() {
        return spliterName;
    }

    public void setSpliterName(String spliterName) {
        this.spliterName = spliterName;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    private XContentBuilder restContentBuilder() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(
                XContentFactory.xContent(XContentType.JSON), out);
        builder.prettyPrint();
        return builder;
    }
}
