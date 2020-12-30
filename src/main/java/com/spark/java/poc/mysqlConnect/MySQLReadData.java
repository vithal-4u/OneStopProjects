package com.spark.java.poc.mysqlConnect;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.constants.ApplicationConstants;
import com.spark.java.poc.utils.SparkUtils;
import com.sun.research.ws.wadl.Application;

public class MySQLReadData implements MySQLConnectivity{

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		MySQLReadData readData = new MySQLReadData();
		Dataset<Row> jdbcDF = readData.createConnection(ApplicationConstants.ONESTOP_SPARK_DB, "select DEFINITION from RULE_BOOK where FILE_NAME='Employee_details'");
		
		/*SparkSession spark = SparkUtils.createSparkSession("MySQLReadData");
		
		Dataset<Row> jdbcDF = spark.read().format("jdbc").option(ApplicationConstants.JDBC_URL_KEY, ApplicationConstants.JDBC_URL_VAL+ApplicationConstants.ONESTOP_SPARK_DB)
			.option(ApplicationConstants.JDBC_DRIVER_KEY, ApplicationConstants.JDBC_DRIVER_VAL)
			//.option("dbtable", "RULE_BOOK")
			.option(ApplicationConstants.JDBC_USER_KEY, ApplicationConstants.JDBC_USER_VAL)
			.option(ApplicationConstants.JDBC_PWD_KEY, ApplicationConstants.JDBC_PWD_VAL)
			.option(ApplicationConstants.JDBC_QUERY_KEY, "select * from RULE_BOOK where FILE_NAME='Employee_details'")
			.load();*/
		jdbcDF.show();
		
	}

}
