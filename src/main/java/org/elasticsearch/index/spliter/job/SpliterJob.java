package org.elasticsearch.index.spliter.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class SpliterJob implements Job{

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        System.out.println("current time:"+new Date());
    }
}
