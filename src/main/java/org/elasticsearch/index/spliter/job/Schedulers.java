package org.elasticsearch.index.spliter.job;

import org.elasticsearch.index.spliter.Spliter;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * @author xingtianyu(code4j) Created on 2018-2-28.
 */
public class Schedulers {

    public static void launchJob(Spliter spliter) throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory();
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
    }

    public static void pauseJob(Spliter spliter) throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory();
        Scheduler scheduler = factory.getScheduler();
        JobKey key = new JobKey(spliter.getSpliterName(),spliter.getSpliterName());
        scheduler.pauseJob(key);
    }

}
