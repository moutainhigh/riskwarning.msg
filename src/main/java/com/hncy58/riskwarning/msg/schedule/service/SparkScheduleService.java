package com.hncy58.riskwarning.msg.schedule.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hncy58.spark2.submit.SparkSubmitApp;

/**
 * Spark任务服务
 * @author	tokings
 * @company	hncy58	湖南长银五八
 * @website	http://www.hncy58.com
 * @version 1.0
 * @date	2019年5月23日 上午10:44:09
 *
 */
@Component
public class SparkScheduleService {
	
	/**
	 * inpala查询url地址
	 */
	@Value("${dbscan.impala.url}")
	private String impalaUrl;
	/**
	 * 结果文件保存路径前缀
	 */
	@Value("${dbscan.file.protocol.prefix}")
	private String fileProtocolPrefix;

	// 进件相关配置
	@Value("${dbscan.entry.appName:EntryCustomerDBSCANCluster}")
	private String entryAppName;
	@Value("${dbscan.entry.distance:1000}")
	private String entryDistance;
	@Value("${dbscan.entry.custCnt:10}")
	private String entryCustCnt;
	@Value("${dbscan.entry.secondsDiffToNow:86400}")
	private String entrySecondsDiffToNow;
	@Value("${dbscan.entry.eventCode:E001}")
	private String entryEventCode;
	@Value("${dbscan.entry.online:1}")
	private String entryOnline;
	@Value("${dbscan.entry.outPath:/tmp/out/entry_dbscan}")
	private String entryOutPath;

	// 授信相关配置
	@Value("${dbscan.credit.appName:CreditCustomerDBSCANCluster}")
	private String creditAppName;
	@Value("${dbscan.credit.distance:1000}")
	private String creditDistance;
	@Value("${dbscan.credit.custCnt:10}")
	private String creditCustCnt;
	@Value("${dbscan.credit.secondsDiffToNow:86400}")
	private String creditSecondsDiffToNow;
	@Value("${dbscan.credit.eventCode:E001}")
	private String creditEventCode;
	@Value("${dbscan.credit.online:1}")
	private String creditOnline;
	@Value("${dbscan.credit.outPath:/tmp/out/credit_dbscan}")
	private String creditOutPath;

	Logger log = LoggerFactory.getLogger(SparkScheduleService.class);

	public void sparkEntryDBScanSubmit() {
		log.info("sparkEntryDBScanSubmit start----------------------");
		log.info("impalaUrl:{}, fileProtocolPrefix:{}", impalaUrl, fileProtocolPrefix);
		String[] args = new String[]{entryAppName, impalaUrl, entryDistance, entryCustCnt, entrySecondsDiffToNow, entryEventCode, entryOnline, entryOutPath, fileProtocolPrefix};
		
		try {
			SparkSubmitApp.main(args);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		log.info("sparkEntryDBScanSubmit end----------------------");
	}

	public void sparkCreditDBScanSubmit() {
		log.info("sparkCreditDBScanSubmit start----------------------");
		log.info("impalaUrl:{}, fileProtocolPrefix:{}", impalaUrl, fileProtocolPrefix);
		try {
			String[] args = new String[]{creditAppName, impalaUrl, creditDistance, creditCustCnt, creditSecondsDiffToNow, creditEventCode, creditOnline, creditOutPath, fileProtocolPrefix};
			SparkSubmitApp.main(args);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		log.info("sparkCreditDBScanSubmit end----------------------");
	}

}
