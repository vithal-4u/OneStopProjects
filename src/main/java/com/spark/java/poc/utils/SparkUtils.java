package com.spark.java.poc.utils;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.constants.ApplicationConstants;

public class SparkUtils  {
	public static SparkConf sparkConf;
	public static SparkSession sparkSession;
	public static JavaSparkContext javaSparkContext;
	public static SparkConf createSparkConfig(String appName) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		if(sparkConf == null) {
			sparkConf = new SparkConf()
					.setAppName(appName)
					.set("spark.driver.allowMultipleContexts",ApplicationConstants.TRUE)
					.setMaster("local[4]")
					.set("spark.sql.warehouse.dir",
							"D:\\Study_Document\\MyPersonal\\spark-warehouse");
		}
		return sparkConf;
	}

	public static SparkSession createSparkSession(String appName) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		/**
		SparkSession sparkSession = SparkSession
				.builder()
				.appName(appName)
				.master("local[2]")
				.config("spark.driver.allowMultipleContexts", "true")
				.config("spark.sql.warehouse.dir",
						"D:\\Study_Document\\MyPersonal\\spark-warehouse").getOrCreate();
						
		*/
		if(sparkSession == null) {
			sparkSession = SparkSession
					.builder()
					//.appName(appName)
					//.master("local[2]")
					.config(SparkUtils.createSparkConfig(appName))
					.getOrCreate();
		}
		return sparkSession;
	}
	
	public static JavaSparkContext createJavaSparkContext(String appName) {
		if(javaSparkContext == null) {
			javaSparkContext = new JavaSparkContext(createSparkConfig(appName));
		}
		return javaSparkContext;
	}
	
	public static  JavaRDD<String> readTextFile(String appName, String fileName,int partitions){
		JavaSparkContext sc = createJavaSparkContext(appName);
		return sc.textFile("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\"+fileName,partitions);
	}
	
	public static  Dataset<Row> readCSVFile(String appName, String fileName){
		sparkSession = SparkSession	.builder()	.config(SparkUtils.createSparkConfig(appName))
					.getOrCreate();
		return sparkSession.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\"+fileName);
	}
	
	public static Dataset<String> readTextFileDataset(String appName, String fileName){
		sparkSession = SparkSession	.builder()	.config(SparkUtils.createSparkConfig(appName))
					.getOrCreate();
		return sparkSession.read().option("header", "true").textFile("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\"+fileName);
	}
}
