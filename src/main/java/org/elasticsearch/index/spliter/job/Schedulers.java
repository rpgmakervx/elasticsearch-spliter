package org.elasticsearch.index.spliter.job;

import org.elasticsearch.index.spliter.Spliter;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

/**
 * @author xingtianyu(code4j) Created on 2018-2-28.
 */
public class Schedulers {

    public static void launchJob(Spliter spliter) throws SchedulerException {
        StdSchedulerFactory factory = new StdSchedulerFactory();
        factory.initialize(new Properties());
        Scheduler scheduler = factory.getScheduler();
        JobDetail detail = JobBuilder.newJob(SpliterJob.class)
                .withDescription(spliter.getDesc())
                .withIdentity(spliter.getSpliterName(),spliter.getSpliterName())
                .build();
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
        StdSchedulerFactory factory = new StdSchedulerFactory();
        factory.initialize(new Properties());
        Scheduler scheduler = factory.getScheduler();
        JobKey key = new JobKey(spliter.getSpliterName(),spliter.getSpliterName());
        scheduler.pauseJob(key);
    }

//    public static void main(String[] args) throws SchedulerException {
//        Spliter spliter = new Spliter();
//        spliter.setSpliterName("dtracker");
//        spliter.setRemain(604800L);
////        spliter.setPeroid("0 0 * * * ? *");
//        spliter.setPeroid("*/1 * * * * ?");
//        spliter.setFormat("yyyyMMdd-HH");
//        spliter.setIndexName("tk_arch_dtracker-");
//        spliter.setAliaName("alia_arch_dtracker");
//        launchJob(spliter);
//    }
}
