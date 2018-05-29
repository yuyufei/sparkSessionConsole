package cn.com.pander.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import cn.com.pander.test.MockData;

public class UserVisitSessionConsole {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]").setAppName("userVisitSessionConsole");
		JavaSparkContext sc=new JavaSparkContext(conf);
		SQLContext sqlContext=SparkSession.builder().config(conf).getOrCreate().sqlContext();
		MockData.mock(sc, sqlContext);
		sc.close();
		
	}

}
