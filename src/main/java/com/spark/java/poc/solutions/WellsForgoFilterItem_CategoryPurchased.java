package com.spark.java.poc.solutions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

/**
 *  Purpose of the class is to read a csv file and filter the customer who purchased Laptop but not TV.
 * 
 * @author kasho
 *
 */
public class WellsForgoFilterItem_CategoryPurchased {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		SparkSession sess = SparkUtils.createSparkSession("WellsForgoFilterItem_CategoryPurchased");
		
		
		Dataset<Row> ds = sess.read().option("header","true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\items.csv");
		ds.show();
		
		ds.select(ds.col("customerId"),ds.col("item_purchased")).distinct().show();
		//.where("item_purchased='laptop' and item_purchased!='tv'").show();
		/*
		//ds.filter(ds.col("item_purchased").notEqual("tv")).filter(ds.col("item_purchased").equalTo("laptop")).show();
		*/
		}

}
