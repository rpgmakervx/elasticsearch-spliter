package org.elasticsearch.index.spliter.job;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.spliter.Spliter;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

/**
 * @author xingtianyu(code4j) Created on 2018-2-28.
 */
public class Schedulers {

    private static StdSchedulerFactory factory;

    static {
        factory = new StdSchedulerFactory();
        Properties properties = new Properties();
        properties.put("org.quartz.threadPool.threadCount","10");
        try {
            factory.initialize(properties);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

    }

    public static void launchJob(Client client, Spliter spliter) throws SchedulerException {
        Scheduler scheduler = factory.getScheduler();
        JobDetail detail = JobBuilder.newJob(SpliterJob.class)
                .withDescription(spliter.getDesc())
                .withIdentity(spliter.getSpliterName(),spliter.getSpliterName())
                .build();
        detail.getJobDataMap().put("spliterName",spliter.getSpliterName());
        detail.getJobDataMap().put("client",client);
        Trigger trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(spliter.getSpliterName(),spliter.getSpliterName())
                .withSchedule(CronScheduleBuilder.cronSchedule(spliter.getPeroid()))
                .build();
        scheduler.scheduleJob(detail,trigger);
        scheduler.start();
        System.out.println("job started");
    }

    public static void pauseJob(Spliter spliter) throws SchedulerException {
        Scheduler scheduler = factory.getScheduler();
        JobKey key = new JobKey(spliter.getSpliterName(),spliter.getSpliterName());
        scheduler.pauseJob(key);
    }

    public static void resumeJob(Spliter spliter) throws SchedulerException {
        Scheduler scheduler = factory.getScheduler();
        JobKey key = new JobKey(spliter.getSpliterName(),spliter.getSpliterName());
        scheduler.resumeJob(key);
    }

}
