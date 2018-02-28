package org.elasticsearch.index.spliter;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperUtils;
import org.elasticsearch.index.opt.Index;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.joda.time.DateTime;

import java.io.IOException;
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

    @Inject
    public ExecuteSpliterAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_spliter/{splitername}/execute", this);
    }

    @Override
    protected void handleRequest(RestRequest restRequest, RestChannel restChannel, Client client) throws Exception {
        super.handleRequest(restRequest,restChannel,client);
        GetRequest getRequest = new GetRequest(INDEX_TMP, TYPE, spliterName);
        GetResponse response = client.get(getRequest).actionGet();
        Map<String,Object> sourceMap = response.getSource();
        Spliter spliter = Spliter.Builder.build(sourceMap);
        if(createIndex(client,spliter)){
            if (bindAlias(client,spliter)){
                deleteIndex(client,spliter);
            }
        }
    }

    private void deleteIndex(Client client,Spliter spliter){
        List<String> indices = Index.getIndices(client,spliter.getIndexName()+WILDCARD);
        if (indices == null){
            return;
        }
        for (String index:indices){
            String createTime = index.replace(spliter.getIndexName(),"");
            DateTime createDateTime = TimeKits.getDateTime(createTime,spliter.getFormat());
            DateTime invalidDateTime = createDateTime.plus(spliter.getRemain());
            //true说明超时，当前索引需要删除
            if (TimeKits.after(DateTime.now(),invalidDateTime)){
                logger.info("id:{} desc:{} delete index name:{}, invalidDateTime:{},format:{}",index,invalidDateTime,spliter.getFormat());
                Index.deleteIndex(client,index);
            }
        }

    }

    private boolean createIndex(Client client,Spliter spliter){
        String dateTime = TimeKits.getCurrentDateFormatted(spliter.getFormat());
        String indexName = spliter.getIndexName() + dateTime;
        boolean created = Index.createIndex(client,indexName);
        if (!created){
            logger.warn("spliter create index fail,indexName [{}]",indexName);
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
        if (!Index.createAlias(client,builder)){
            logger.error("spliter[{}] rebind alias fail, rest body is:\n{}",spliterName,builder.string());
            return false;
        }
        return true;
    }

}
