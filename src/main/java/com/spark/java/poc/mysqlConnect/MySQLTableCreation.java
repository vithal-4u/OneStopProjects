package com.spark.java.poc.mysqlConnect;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class MySQLTableCreation {

	public static void main(String[] args) {

		//Details related to MySQL connectivity
		Properties prop = new Properties();
		prop.setProperty("driver", "com.mysql.jdbc.Driver");
		prop.setProperty("dbtable", "Student_Marks");
		prop.setProperty("user", "root");
		prop.setProperty("password", "ayyappasai");
		
		SparkSession spark = SparkUtils.createSparkSession("MySQLWriteData");
		Dataset<Row> df = spark.read().jdbc("jdbc:mysql://localhost:3306/ONESTOP_SPARK_DB", "tempTable", prop);
		df.write().option("createTableColumnTypes", "name VARCHAR(500), col1 VARCHAR(1024), col3 int").jdbc("jdbc:mysql://localhost:3306/ONESTOP_SPARK_DB", "tempTable", prop);
		System.out.println("Finish");
	}

}
