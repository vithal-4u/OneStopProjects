package com.spark.java.poc.mysqlConnect;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.constants.ApplicationConstants;
import com.spark.java.poc.utils.SparkUtils;

public interface MySQLConnectivity {
	
	default Dataset<Row> createConnection(String DB,String query){
		SparkSession spark = SparkUtils.createSparkSession("MySQLReadData");
		Dataset<Row> jdbcDF = spark.read().format("jdbc").option(ApplicationConstants.JDBC_URL_KEY, ApplicationConstants.JDBC_URL_VAL+DB)
			.option(ApplicationConstants.JDBC_DRIVER_KEY, ApplicationConstants.JDBC_DRIVER_VAL)
			//.option("dbtable", "RULE_BOOK")
			.option(ApplicationConstants.JDBC_USER_KEY, ApplicationConstants.JDBC_USER_VAL)
			.option(ApplicationConstants.JDBC_PWD_KEY, ApplicationConstants.JDBC_PWD_VAL)
			.option(ApplicationConstants.JDBC_QUERY_KEY, query)
			.load();
		return jdbcDF;
	}
}
