package org.elasticsearch.index.opt;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.spliter.SpliterConstant;
import org.elasticsearch.index.spliter.SpliterIndexMapper;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
public class Index {

    private static final ESLogger logger = Loggers.getLogger(Index.class);

    public static boolean createIndex(Client client, String indexName){
        CreateIndexRequest request = new CreateIndexRequest();
        request.index(indexName);
        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        return response.isAcknowledged();
    }

    public static boolean createAlias(Client client,String aliasName,String newIndex) throws IOException {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias(aliasName,newIndex);
        IndicesAliasesResponse response = client.admin().indices().aliases(request).actionGet();
        return response.isAcknowledged();
    }
    public static boolean rebindAlias(Client client,String aliasName,String newIndex,List<String> indices) throws IOException {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias(aliasName,newIndex);
        request.removeAlias(indices.toArray(new String[indices.size()]),aliasName);
        IndicesAliasesResponse response = client.admin().indices().aliases(request).actionGet();
        return response.isAcknowledged();
    }

    public static void createMapper(Client client,String indexName,String type,XContentBuilder mapper) throws IOException {
        mapper.startObject()
                .startObject(type)
                .startObject(SpliterConstant.PROPERTIES);
        for(Map.Entry<String,Map<String,String>> entry: SpliterIndexMapper.mapper.entrySet()){
            mapper.startObject(entry.getKey());
            for (Map.Entry<String,String> metaEntry:entry.getValue().entrySet()){
                mapper.field(metaEntry.getKey(),metaEntry.getValue());
            }
            mapper.endObject();
        }
        mapper.endObject().endObject().endObject();
        PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).type(type).source(mapper);
        client.admin().indices().putMapping(mappingRequest).actionGet();
    }

    public static List<String> getIndices(Client client,String pattern){
        GetIndexRequest request = new GetIndexRequest();
        request.indices(pattern);
        GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();
        return Arrays.asList(response.indices());
    }

    public static boolean exists(Client client,String indexName){
        IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
        IndicesExistsResponse existsResponse = client.admin().indices().exists(existsRequest).actionGet();
        return existsResponse.isExists();
    }

    public static void deleteIndex(Client client,String indexName){
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices(indexName);
        client.admin().indices().delete(request).actionGet();
    }

}
