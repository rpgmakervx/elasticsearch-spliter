package org.elasticsearch.index.spliter;

import java.util.Map;

/**
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
public class Spliter {

    private String indexName;

    private String aliaName;

    private String peroid;

    private String format;

    private Long remain;


    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getAliaName() {
        return aliaName;
    }

    public void setAliaName(String aliaName) {
        this.aliaName = aliaName;
    }

    public String getPeroid() {
        return peroid;
    }

    public void setPeroid(String peroid) {
        this.peroid = peroid;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Long getRemain() {
        return remain;
    }

    public void setRemain(Long remain) {
        this.remain = remain;
    }

    public static class Builder{

        public static Spliter build(Map<String,Object> sourceMap){
            Spliter spliter = new Spliter();
            String indexPrefix = String.valueOf(sourceMap.get(SpliterConstant.INDEX_NAME));
            String aliaName = String.valueOf(sourceMap.get(SpliterConstant.ALIA_NAME));
            String peroid = String.valueOf(sourceMap.get(SpliterConstant.PERIOD));
            String format = String.valueOf(sourceMap.get(SpliterConstant.FORMAT));
            Long remain = Long.parseLong(String.valueOf(sourceMap.get(SpliterConstant.REMAIN)));
            spliter.setAliaName(aliaName);
            spliter.setFormat(format);
            spliter.setIndexName(indexPrefix);
            spliter.setPeroid(peroid);
            spliter.setRemain(remain);
            return spliter;
        }
    }
}
