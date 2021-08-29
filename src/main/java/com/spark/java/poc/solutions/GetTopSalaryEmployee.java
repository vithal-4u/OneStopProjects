package com.spark.java.poc.solutions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

/**
 * Class to read a csv file and convert the file into dataset and get top 6 highest paid employee salary
 * 
 * @author kasho
 *
 */
public class GetTopSalaryEmployee {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		SparkSession sparkCxt = SparkUtils.createSparkSession("AirportsByLatitudeSolutions");
		Dataset<Row> dataset = sparkCxt.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\emp_data.txt");
		dataset.show();
		dataset.printSchema();
		
		dataset.select(dataset.col("sal")).show();
	}

}
