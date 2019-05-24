package com.hncy58.spark2.dbscan;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.hncy58.impala.ImpalaJDBC;
import com.hncy58.spark.dbscan.Dbscan;
import com.hncy58.spark.dbscan.DbscanModel;
import com.hncy58.spark.dbscan.DbscanSettings;
import com.hncy58.spark.dbscan.distance.GEODistance;
import com.hncy58.spark.dbscan.spatial.Point;
import com.hncy58.spark.dbscan.spatial.rdd.PartitioningSettings;
import com.hncy58.spark.dbscan.util.io.IOHelper;
import com.hncy58.spark2.dbscan.invoker.MsgSendInvoker;

public class CreditCustomerDBSCANCluster {

	private static String appName = CreditCustomerDBSCANCluster.class.getSimpleName();
	private static String outPath = "/tmp/out/entry_dbscan";
	private static String fileProtocolPrefix = "hdfs://node01:8020";
	private static String impalaUrl = "jdbc:impala://127.0.0.1:21050";
	private static double distance = 2000.00;
	private static int custCnt = 5;
	private static int secondsDiffToNow = 24 * 60 * 60;
	private static String eventCode = "E001";
	private static String online = "1";
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	public static void main(String[] args) {
		
		String now = sdf.format(new Date());

		System.out.println("HADOOP_HOME ENV => " + System.getenv("HADOOP_HOME"));
		System.out.println("hadoop.home.dir ENV => " + System.getenv("hadoop.home.dir"));

		// 接收命令行参数配置
		if (args.length > 0)
			appName = args[0].trim();
		if (args.length > 1)
			impalaUrl = args[1].trim();
		if (args.length > 2)
			distance = Double.parseDouble(args[2].trim());
		if (args.length > 3)
			custCnt = Integer.parseInt(args[3].trim());
		if (args.length > 4)
			secondsDiffToNow = Integer.parseInt(args[4].trim());
		if (args.length > 5)
			eventCode = args[5].trim();
		if (args.length > 6)
			online = args[6].trim();
		if (args.length > 7)
			outPath = args[7].trim();
		if (args.length > 8)
			fileProtocolPrefix = args[8].trim();

		System.out.println("DBSCAN algorithm based on spark2 distributed ENV.");
		System.out.printf("Usage: %s appName impalaUrl distance custCnt secondsDiffToNow eventCode online outPath fileProtocolPrefix", CreditCustomerDBSCANCluster.class.getSimpleName());
		System.out.println();
		System.out.printf("eg: %s %s %s %s %s %s %s %s %s %s", CreditCustomerDBSCANCluster.class.getSimpleName(), appName, impalaUrl, distance, custCnt, secondsDiffToNow, eventCode, online, outPath, fileProtocolPrefix);
		System.out.println();

		// 查询secondsDiffToNow内的进件客户且曾经授信通过时的GPS地址
		String sql = "select c.cert_id,lbs_longitude,t.lbs_latitude "
				+ "from kudu_ods_riskcontrol.inf_customer_credit c "
				+ "left join kudu_ods_isop.inf_customer_expand c1 on c.cert_id = c1.cert_id "
				+ "left join kudu_ods_riskcontrol.inf_address t on t.busi_seq = c.task_id "
				+ "where "
				+ "    c.first_apply_date between from_unixtime(unix_timestamp() - " + secondsDiffToNow + ") and from_unixtime(unix_timestamp()) "	//	进件时间范围
				+ "    and c.start_date is not null "	//	曾经授信通过
				+ "    and c1.online = " + online	//	线上/线下
				+ "    and t.event_code = '" + eventCode + "' "	//	事件编码
				+ "    and t.lbs_longitude is not null and trim(t.lbs_longitude) != '' "	//	客户GPS经度不为空
				+ "    and t.lbs_latitude is not null and trim(t.lbs_latitude) != '' "	//	客户GPS纬度不为空
				;
		
		System.out.println("impala query sql:" + sql);

		SparkConf conf = new SparkConf()
//				.setAppName("CreditCustomerDBSCANCluster")
//				.setMaster("local[6]")	// for local test
				.set("spark.driver.userClassPathFirst", "true")
				.set("spark.sql.crossJoin.enabled", "true")
				;

		SparkContext sc = null;
		JavaSparkContext jsc = null;
		long start;
		
		try {
			sc = new SparkContext(conf);
			jsc = new JavaSparkContext(sc);
			
			start = System.currentTimeMillis();
			List<Map<String, Object>> list = ImpalaJDBC.queryForList(impalaUrl, sql);
			
			if(list.isEmpty() || list.size() < custCnt) {
				System.out.println("impala query ret is empty or little than " + custCnt);
			} else {
				System.out.println("impala query ret size:" + list.size());
				
				JavaRDD<Point> dsPoints = jsc.parallelize(list).map(map -> {
					return new Point(map.get("cert_id").toString(), new double[] {Double.valueOf(map.get("lbs_latitude").toString()), Double.valueOf(map.get("lbs_longitude").toString()) });
				});
				// 聚类算法配置
				DbscanSettings clusteringSettings = new DbscanSettings().withEpsilon(distance).withNumberOfPoints(custCnt);
				clusteringSettings.withTreatBorderPointsAsNoise(true);
				clusteringSettings.withDistanceMeasure(new GEODistance()); // 修改为GEO距离计算
				
				start = System.currentTimeMillis();
				// 训练数据模型
				DbscanModel model = Dbscan.train(dsPoints.rdd(), clusteringSettings,
						new PartitioningSettings(PartitioningSettings.DefaultNumberOfSplitsAlongEachAxis(),
								PartitioningSettings.DefaultNumberOfLevels(), PartitioningSettings.DefaultNumberOfPointsInBox(),
								PartitioningSettings.DefaultNumberOfSplitsWithinPartition()));
				System.out.println("model trained started, used " + (System.currentTimeMillis() - start)
						+ " ms.========================================");
				
				//	聚类结果回调
				System.out.println("invoker ret:" + new MsgSendInvoker().invoke(model.allPoints().toJavaRDD().collect()));
				//	保存到文件
				saveToFile(sc, model, fileProtocolPrefix, outPath + "/" + now);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			start = System.currentTimeMillis();
			jsc.close();
			sc.stop();
			System.out.println("sparkcontext stoped, used " + (System.currentTimeMillis() - start)
					+ " ms.========================================");
		}
	}

	private static void saveToFile(SparkContext sc, DbscanModel model, String protocolPrefix, String path) {
		// 删除输出目录
		IOHelper.deleteOutPath(sc, fileProtocolPrefix, path);
		//	保存结果到文件
		long start = System.currentTimeMillis();
		IOHelper.saveClusteringResult(model, fileProtocolPrefix + path);
		System.out.println("model saved, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
	}
}
