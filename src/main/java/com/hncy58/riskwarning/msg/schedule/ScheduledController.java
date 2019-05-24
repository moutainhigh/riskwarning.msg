package com.hncy58.riskwarning.msg.schedule;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hncy58.riskwarning.msg.schedule.service.ScheduleService;
import com.hncy58.riskwarning.msg.schedule.service.SparkScheduleService;

/**
 * 定时任务控制器
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年12月11日 上午11:15:46
 *
 */
@Component
@Async
public class ScheduledController {

	Logger log = LoggerFactory.getLogger(ScheduledController.class);

	@Autowired
	ScheduleService scheduledService;
	
	@Autowired
	SparkScheduleService sparkScheduleService;
	
	@Scheduled(cron = "${dbscan.entry.cron}")
	public void sparkEntryDBScanSubmit() {
		sparkScheduleService.sparkEntryDBScanSubmit();
	}
	
	@Scheduled(cron = "${dbscan.credit.cron}")
	public void sparkCreditDBScanSubmit() {
		sparkScheduleService.sparkCreditDBScanSubmit();
	}

//	@Scheduled(cron = "${dbscan.entry.cron}")
//	public void scheduled1() {
//		log.info("=====>>>>>使用dbscan.entry.cron, {}", new Date());
//	}
//	
//	@Scheduled(cron = "${dbscan.credit.cron}")
//	public void scheduled2() {
//		log.info("=====>>>>>使用dbscan.credit.cron, {}", new Date());
//	}

}
