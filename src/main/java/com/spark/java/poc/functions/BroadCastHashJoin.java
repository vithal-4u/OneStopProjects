package com.spark.java.poc.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class BroadCastHashJoin {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		SparkSession sparkSess = SparkUtils.createSparkSession("BroadCastHashJoin");
		Dataset<Row> empSample = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\Empl_sample_5.csv");
		Dataset<Row> empAddr = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\Empl_Addr_5.csv");
		
		}

}
