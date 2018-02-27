package org.elasticsearch.index.spliter.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

import java.util.Date;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class SpliterJob implements Job{

    private boolean terminate = false;

    public static JobKey jobKey;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        jobKey = context.getJobDetail().getKey();
        System.out.println("current time:"+new Date());
    }

}
