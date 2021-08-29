package com.spark.java.poc.sql.functions;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class MinusFunctionDatasets {

	public static void main(String[] args) {
		SparkSession sparkSess = SparkUtils.createSparkSession("MinusFunctionDatasets");
		Dataset<Row> empFileData = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\Customer_Items.csv");
		Dataset<Row> empFileData2 = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\Customer_Items.csv");
		try {
			//Dataset<Row> dsTvItems =empFileData.where(Column.class.equals("item_name = 'tv'"));
			//dsTvItems.show();
			empFileData.except(empFileData2).show();
			 empFileData.createTempView("Customer_Items1");
			 empFileData.createTempView("Customer_Items2");
			 
			 //Dataset<Row> sqlDF = sparkSess.sql("select customer_id from Customer_Items1 where item_name='tv' MINUS  select customer_id from Customer_Items2 where item_name='refrigerator'");
			 //sqlDF.show();
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}

}
