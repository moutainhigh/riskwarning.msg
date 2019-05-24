package com.hncy58.riskwarning.msg.cfg;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年12月11日 上午11:23:33
 *
 */
@Configuration
@EnableScheduling
@EnableAsync
public class AsyncScheduleConfig {

	@Value("${schedule.corePoolSize:10}")
	private int corePoolSize;

	@Value("${schedule.maxPoolSize:200}")
	private int maxPoolSize;

	@Value("${schedule.queueCapacity:10}")
	private int queueCapacity;

	@Bean
	public Executor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(corePoolSize);
		executor.setMaxPoolSize(maxPoolSize);
		executor.setQueueCapacity(queueCapacity);
		executor.initialize();
		return executor;
	}
}