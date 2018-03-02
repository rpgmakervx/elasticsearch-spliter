package org.elasticsearch.index.spliter.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.concurrent.Executors;

/**
 * @author xingtianyu(code4j) Created on 2018-3-1.
 */
public class TestJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            System.out.println("exception job");
            throw new NullPointerException();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
