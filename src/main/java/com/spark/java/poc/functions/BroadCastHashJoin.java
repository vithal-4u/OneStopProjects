package com.spark.java.poc.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;

public class BroadCastHashJoin {

	public static void main(String[] args) throws AnalysisException {
		
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		SparkSession sparkSess = SparkUtils.createSparkSession("BroadCastHashJoin");
		Dataset<Row> empSample = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\Empl_sample_5.csv");
		Dataset<Row> empAddr = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\Empl_Addr_5.csv");
		
		empSample.createTempView("EmpSampleTbl");
		empAddr.createTempView("EmpAddrTbl");
		//empSample.join(broadcast(empAddr), col("empSample.emp_id").equalTo(col("empAddr.emp_id"))).select(col("empSample.*")).show();
		empSample.join(broadcast(empAddr), empSample.col("emp_id").equalTo(empAddr.col("emp_id"))).select(empSample.col("emp_id"),empSample.col("city"),empSample.col("name"),empSample.col("dep"),empAddr.col("Addr")).show();
		sparkSess.sql("Select a.emp_id,a.name, a.city,a.dep,b.Addr from EmpSampleTbl a join EmpAddrTbl b where a.emp_id=b.emp_id").show();
		}

}

