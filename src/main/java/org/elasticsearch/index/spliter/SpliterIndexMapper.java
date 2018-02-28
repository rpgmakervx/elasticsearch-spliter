package org.elasticsearch.index.spliter;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class SpliterIndexMapper {

    public static final Map<String,Map<String,String>> mapper = new HashMap<>();

    static {
        Map<String,String> keywordMapper = Maps.newHashMap();
        Map<String,String> textMapper = Maps.newHashMap();
        Map<String,String> intMapper = Maps.newHashMap();
        Map<String,String> longMapper = Maps.newHashMap();
        keywordMapper.put("type","string");
        keywordMapper.put("index","not_analyzed");
        textMapper.put("type","string");
        intMapper.put("type","integer");
        longMapper.put("type","long");
        mapper.put(SpliterConstant.ALIA_NAME, keywordMapper);
        mapper.put(SpliterConstant.INDEX_NAME,keywordMapper);
        mapper.put(SpliterConstant.FORMAT,keywordMapper);
        mapper.put(SpliterConstant.PERIOD,keywordMapper);
        mapper.put(SpliterConstant.REMAIN,longMapper);
        mapper.put(SpliterConstant.DESC,textMapper);
    }

}
