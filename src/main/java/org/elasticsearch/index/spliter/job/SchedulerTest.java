package org.elasticsearch.index.spliter.job;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

/**
 * @author xingtianyu(code4j) Created on 2018-2-26.
 */
public class SchedulerTest {

    public static void main(String[] args) throws SchedulerException, InterruptedException {
        SchedulerFactory factory = new StdSchedulerFactory();
        Scheduler scheduler = factory.getScheduler();
        JobDetail detail = JobBuilder.newJob(SpliterJob.class)
                .withDescription("test job")
                .withIdentity("spliterjob","splitergroup")
                .build();
//        Date delay = new Date(System.currentTimeMillis()+3000);
        Trigger trigger = TriggerBuilder.newTrigger()
                .withDescription("")
                .withIdentity("spliterTrigger","spliterTriggerGroup")
                .startAt(new Date())
                .withSchedule(CronScheduleBuilder.cronSchedule("0/1 * * * * ?"))
                .build();
        scheduler.scheduleJob(detail,trigger);
        scheduler.start();
        Thread.sleep(1000);
        Thread.sleep(1000);
        Thread.sleep(1000);
        System.out.println("pause job");
        scheduler.pauseJob(SpliterJob.jobKey);
        Thread.sleep(1000);
        Thread.sleep(1000);
        Thread.sleep(1000);
        Thread.sleep(1000);
        Thread.sleep(1000);
        System.out.println("resume job");
        scheduler.resumeJob(SpliterJob.jobKey);
        Thread.sleep(1000);
        Thread.sleep(1000);
        Thread.sleep(1000);
        System.out.println("delete job");
        scheduler.deleteJob(SpliterJob.jobKey);
        Thread.sleep(1000);
        Thread.sleep(1000);
        Thread.sleep(1000);
        System.out.println("resume job");
        scheduler.resumeJob(SpliterJob.jobKey);

    }

}
