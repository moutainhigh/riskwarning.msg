package com.hncy58.spark2.submit;

import java.util.HashMap;

import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 应用程序内部提交Spark任务
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年5月21日 上午11:43:36
 *
 */
@Service
public class SparkSubmitApp {
	
	private static final Logger log = LoggerFactory.getLogger(SparkSubmitApp.class);
	
	/**
	 * spark执行用户，默认为hdfs
	 */
	@Value("${spark.executor.user:hdfs}")
	private String sparkExecUser;
	
	@Value("${hadoop.conf.dir}")
	private String hadoopConfDir;
	
	@Value("${yarn.conf.dir}")
	private String yarnConfDir;
	
	@Value("${spark.home}")
	private String sparkHome;
	
	@Value("${java.home}")
	private String javaHome;

	@Value("${spark.driver.cores}")
	private String driverCores;
	
	@Value("${spark.driver.memory}")
	private String driverMemory;
	
	@Value("${spark.executor.cores}")
	private String executorCores;
	
	@Value("${spark.executor.memory}")
	private String executorMemory;
	
	@Value("${spark.num.executors}")
	private String numExecutors;

	@Value("${spark.cores.max}")
	private String coresMax;

	@Value("${spark.dynamicAllocation.enabled}")
	private String dynamicAllocationEnable;
	
	@Value("${spark.dynamicAllocation.initialExecutors}")
	private String initialExecutors;
	
	@Value("${spark.dynamicAllocation.minExecutors}")
	private String minExecutors;
	
	@Value("${spark.dynamicAllocation.maxExecutors}")
	private String maxExecutors;
	
	@Value("${spark.main.res.path}")
	private String appRes;
	
	@Value("${spark.master:yarn}")
	private String master;
	
	@Value("${spark.deploy.mode:cluster}")
	private String deployMode;

	@Value("${spark.executer.files}")
	private String files;
	
	@Value("${spark.executer.jars}")
	private String jars;
	
	private String mainClass = "";
	
	String appName = SparkSubmitApp.class.getSimpleName();
	
	public void submit(String[] args) {
		
		if(args.length < 1) {
			log.error("main class must be seted.");
			return;
		} 
		
		mainClass = args[0].trim();
		appName = mainClass;
		
		log.info("main class:" + mainClass);
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		// 生产环境通过单独的spark.env文件进行定义
		String tmpEvn = System.getenv("HADOOP_CONF_DIR");
		if(tmpEvn == null || "".equals(tmpEvn)) {
			map.put("HADOOP_CONF_DIR", hadoopConfDir);
		}
		
		tmpEvn = System.getenv("YARN_CONF_DIR");
		if(tmpEvn == null || "".equals(tmpEvn)) {
			map.put("YARN_CONF_DIR", yarnConfDir);
		}
		
		tmpEvn = System.getenv("SPARK_HOME");
		if(tmpEvn == null || "".equals(tmpEvn)) {
			map.put("SPARK_HOME", sparkHome);
		}
		
		tmpEvn = System.getenv("JAVA_HOME");
		if(tmpEvn == null || "".equals(tmpEvn)) {
			 map.put("JAVA_HOME", javaHome);
		}
		
		// 设置执行器任务执行用户
		tmpEvn = System.getenv("HADOOP_USER_NAME");
		if(tmpEvn == null || "".equals(tmpEvn)) {
			map.put("HADOOP_USER_NAME", sparkExecUser);
		}

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
					// 设置驱动器执行内存
					.setConf("spark.driver.memory", driverMemory)
					// 设置驱动器执行内存
					.setConf("spark.driver.cores", driverCores)
					// 设置执行器执行核数
					.setConf("spark.executor.cores", executorCores)
					// 设置执行器执行实例数
					.setConf("spark.num.executors", numExecutors)
					.setConf("spark.executor.instances", numExecutors)
					// 设置执行器执行内存
					.setConf("spark.executor.memory", executorMemory)
					// 设置执行器执行内存
					.setConf("spark.cores.max", coresMax)
					
					.setConf("spark.dynamicAllocation.enabled", dynamicAllocationEnable)
					.setConf("spark.dynamicAllocation.initialExecutors", initialExecutors)
					.setConf("spark.dynamicAllocation.minExecutors", minExecutors)
					.setConf("spark.dynamicAllocation.maxExecutors", maxExecutors)
					// 以下配置和上述配置等同
//					.addSparkArg("--total-executor-cores=10")
//					.addSparkArg("--num-executors=4")
//					.addSparkArg("--executor-cores=2")
//					.addSparkArg("--executor-memory=1g")
//					.addSparkArg("--driver-memory=2g")
//					.addSparkArg("--driver-cores=2")

					.setConf("spark.yarn.preserve.staging.files", "true")
					.setConf("spark.sql.session.timeZone", "Asia/Shanghai").setVerbose(true)
					;

			// 添加部署应用所需的依赖jar
			if (jars != null && !"".equals(jars.trim())) {
				for(String jar : jars.split(" *, *")) {
					spark.addJar(jar);
				}
			}
			
			// 添加部署应用所需的相关文件
			if (files != null && !"".equals(files.trim())) {
				for(String file : files.split(" *, *")) {
					spark.addFile(file);
				}
			}
			
			// 添加主应用程序参数
			spark.addAppArgs(args);
	
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
