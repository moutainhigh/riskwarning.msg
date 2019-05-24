package com.hncy58.spark2.dbscan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.hncy58.spark.dbscan.Dbscan;
import com.hncy58.spark.dbscan.DbscanModel;
import com.hncy58.spark.dbscan.DbscanSettings;
import com.hncy58.spark.dbscan.distance.GEODistance;
import com.hncy58.spark.dbscan.spatial.Point;
import com.hncy58.spark.dbscan.spatial.rdd.PartitioningSettings;
import com.hncy58.spark.dbscan.util.io.IOHelper;

public class DBSCANClusterWithImpalaData {

	// private static String inPath =
	// "C:/dev/workspace/spark2-submit/inf_address_20190516.csv";
	// private static String outPath = "C:/dev/workspace/spark2-submit/out";
	// private static String fileProtocolPrefix = "file:///";
	// private static boolean localMode = true;
	private static String inPath = "/tmp/inf_address_20190516.csv";
	private static String outPath = "/tmp/out";
	private static String fileProtocolPrefix = "hdfs://node01:8020";
	
	private static String impalaUrl = "jdbc:impala://node01:21050";

	public static void main(String[] args) {

		System.out.println("HADOOP_HOME ENV => " + System.getenv("HADOOP_HOME"));
		System.out.println("HADOOP_HOME PROP => " + System.getProperty("HADOOP_HOME"));
		System.out.println("hadoop.home.dir ENV => " + System.getenv("hadoop.home.dir"));
		System.out.println("hadoop.home.dir PROP => " + System.getProperty("hadoop.home.dir"));

		System.out.println("DBSCAN algorithm based on spark2 distributed ENV.");
		System.out.printf("Usage: %s inpath outPath fileProtocolPrefix", DBSCANClusterWithImpalaData.class.getSimpleName());
		System.out.println();
		System.out.printf("Usage: %s %s %s %s", DBSCANClusterWithImpalaData.class.getSimpleName(), inPath, outPath, fileProtocolPrefix);
		System.out.println();

		// 接收命令行参数配置
		if (args.length > 0)
			inPath = args[0].trim();
		if (args.length > 1)
			outPath = args[1].trim();
		if (args.length > 2) {
			fileProtocolPrefix = args[2].trim();
		}

		SparkConf conf = new SparkConf()
				.setAppName("DBSCANClusterWithImpalaData")
				.setMaster("local[6]")	// for local test
				.set("spark.driver.userClassPathFirst", "true")
				.set("spark.sql.crossJoin.enabled", "true")
				;

		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);

		List<Map<String, Object>> list = new ArrayList<>();
		
		if(list.isEmpty() || list.size() < 2) {
			System.out.println("query ret is empty or little than 2");
		}
		
//		List<Map<String, Object>> list = ImpalaJDBC.queryForList(impalaUrl, "select * from kudu_ods_riskcontrol.inf_address");
		JavaRDD<Point> dsPoints = jsc.parallelize(list).map(map -> {
			return new Point(map.get("id").toString(), new double[] {Double.valueOf(map.get("lbs_longitude").toString()), Double.valueOf(map.get("lbs_latitude").toString()) });
		});
		// 聚类算法配置
		DbscanSettings clusteringSettings = new DbscanSettings().withEpsilon(50000000).withNumberOfPoints(2);
		clusteringSettings.withTreatBorderPointsAsNoise(true);
		clusteringSettings.withDistanceMeasure(new GEODistance()); // 修改为GEO距离计算

		long start = System.currentTimeMillis();
		// 训练数据模型
		DbscanModel model = Dbscan.train(dsPoints.rdd(), clusteringSettings,
				new PartitioningSettings(PartitioningSettings.DefaultNumberOfSplitsAlongEachAxis(),
						PartitioningSettings.DefaultNumberOfLevels(), PartitioningSettings.DefaultNumberOfPointsInBox(),
						PartitioningSettings.DefaultNumberOfSplitsWithinPartition()));
		System.out.println("model trained started, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
		
		// 打印结果
		model.allPoints().toJavaRDD().foreach(p -> System.out.println(p.id() + " -> " + p));

		// 删除输出目录
		IOHelper.deleteOutPath(sc, fileProtocolPrefix, outPath);
		
		//	保存结果到文件
		start = System.currentTimeMillis();
		IOHelper.saveClusteringResult(model, fileProtocolPrefix + outPath);
		System.out.println("model saved, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");

		start = System.currentTimeMillis();
		jsc.close();
		sc.stop();
		System.out.println("sparkcontext stoped, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
	}
}
