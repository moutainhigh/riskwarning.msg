package com.hncy58.spark2;

import java.util.HashMap;

import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.spark2.submit.InputStreamReaderRunnable;

public class SparkSubmitTest {

	private static final Logger log = LoggerFactory.getLogger(SparkSubmitTest.class);
	// 授信相关配置
	private static String impalaUrl = "jdbc:impala://100.66.70.202:21050";
	private static String fileProtocolPrefix = "hdfs://hncy58";
	private static String creditAppName = "com.hncy58.spark2.dbscan.CreditCustomerDBSCANCluster";
	private static String creditDistance = "100000";
	private static String creditCustCnt = "3";
	private static String creditSecondsDiffToNow = "8640000";
	private static String creditEventCode = "E001";
	private static String creditOnline = "1";
	private static String creditOutPath = "/tmp/out/credit_dbscan";

	public static void main(String[] args) {

		args = new String[] { creditAppName, impalaUrl, creditDistance, creditCustCnt, creditSecondsDiffToNow,
				creditEventCode, creditOnline, creditOutPath, fileProtocolPrefix };

		String execUser = "hdfs";
		String appRes = "file:/D:/dev/svn/bigdata1/code/riskwarning-sms/target/riskwarning.msg-0.0.1.jar.original";
		String mainClass = "com.hncy58.spark2.dbscan.CreditCustomerDBSCANCluster";
		String deployMode = "cluster";
		String master = "yarn";
		String appName = SparkSubmitTest.class.getSimpleName();

		if (args.length > 0) {
			mainClass = args[0].trim();
			appName = mainClass;
		}

		log.info("main class:" + mainClass);

		HashMap<String, String> map = new HashMap<String, String>();

		// 通过单独的spark.env文件进行定义
		 map.put("HADOOP_CONF_DIR", "F:/hadoop-2.6.5/etc/hadoop");
		 map.put("SPARK_HOME", "F:/spark");
		// map.put("JAVA_HOME", "/usr/java/jdk1.8.0_141-cloudera");

		// 设置执行器任务执行用户
		map.put("HADOOP_USER_NAME", execUser);

		try {
			SparkLauncher spark = new SparkLauncher(map)
					// 设置应用程序名称
					.setAppName(appName)
					// 设置部署模式
					.setMaster(master)
					// 设置执行模式
					.setDeployMode(deployMode)
					// 设置主应用程序jar路径
					.setAppResource(appRes)
					// 设置主应用程序执行类
					.setMainClass(mainClass)
					// // 设置驱动器执行内存
					// .setConf("spark.driver.memory", "2g")
					// // 设置驱动器执行内存
					// .setConf("spark.driver.cores", "2")
					// // 设置执行器执行核数
					// .setConf("spark.executor.cores", "2")
					// // 设置执行器执行实例数
					// .setConf("spark.num.executors", "4")
					// // 设置执行器执行内存
					// .setConf("spark.executor.memory", "1g")
					// // 设置执行器执行内存
					// .setConf("spark.cores.max", "16")
					.addSparkArg("--total-executor-cores=10").addSparkArg("--num-executors=4")
					.addSparkArg("--executor-cores=2").addSparkArg("--executor-memory=1g")
					.addSparkArg("--driver-memory=2g").addSparkArg("--driver-cores=2")

					.setConf("spark.yarn.preserve.staging.files", "true")
					.setConf("spark.sql.session.timeZone", "Asia/Shanghai").setVerbose(true)

					// 添加部署应用所需的依赖jar
					.addJar("file:/D:/dev/svn/bigdata1/code/riskwarning-sms/lib/scala-dbscan-0.0.1.jar")
					.addJar("file:/D:/dev/svn/bigdata1/code/riskwarning-sms/lib/kudu-spark2_2.11-1.6.0-cdh6.0.1.jar")
					.addJar("file:/D:/dev/svn/bigdata1/code/riskwarning-sms/lib/ImpalaJDBC41.jar")
					// 添加部署应用所需的相关文件
					// .addFile("./log4j.properties")
					// 添加主应用程序参数
					.addAppArgs(args);

			// 启动spark任务
			log.info("启动spark任务");
			Process process = spark.launch();
			InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(
					process.getInputStream(), "input");
			Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
			inputThread.start();

			InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(
					process.getErrorStream(), "error");
			Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
			errorThread.start();

			log.info("Waiting for finish...");
			int exitCode = process.waitFor();
			log.info("Finished! Exit code:" + exitCode);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
