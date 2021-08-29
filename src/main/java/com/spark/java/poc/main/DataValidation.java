package com.spark.java.poc.main;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.mysqlConnect.MySQLReadQueryBased;
import com.spark.java.poc.pojo.Employee;
import com.spark.java.poc.pojo.RuleBookColumnsPojo;
import com.spark.java.poc.utils.SparkUtils;

public class DataValidation {

	public static void main(String[] args) {
		MySQLReadQueryBased readQueryBased = new MySQLReadQueryBased();
		JavaRDD<String> ruleBookRDD = readQueryBased.getDatafromRuleBookTableStr("select DEFINITION from RULE_BOOK where FILE_NAME='Employee_details'");
		//SparkSession spark = SparkUtils.createSparkSession("MySQLWriteData");
		JavaSparkContext sc = SparkUtils.createJavaSparkContext("MySQLEmployeeData");
		Broadcast<List<String>> broadcastedFieldNames = sc.broadcast(ruleBookRDD.collect());
		
//		JavaRDD<String> words = broadcastedFieldNames.value().flatMap(new FlatMapFunction<String, String>(){
//
//			@Override
//			public Iterator<String> call(String x) throws Exception {
//				System.out.println("Data Inside FlatMap---"+ x);
//				return Arrays.asList(x).iterator();
//			}
//			
//		});
		System.out.println("Broadcast Variable -- "+((List<String>)broadcastedFieldNames.getValue()).get(0));
		//words.collect();
		System.exit(0);
		SparkSession spark = SparkUtils.createSparkSession("MySQLWriteData");
		JavaRDD<Row> csvDS = spark.read().format("csv").option("header","true").load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\employee_sample.csv").toJavaRDD();
		
		JavaRDD<Employee> employeeDF =csvDS.map(new Function<Row, Employee>() {
			@Override
			public Employee call(Row rowData) throws Exception {
				System.out.println(rowData.getString(0)+"--"+rowData.getString(1) +"--"+ rowData.getString(2) +"--"+ rowData.getString(3) +"--"+ rowData.getString(4));
				return new Employee(Integer.parseInt(rowData.getString(0)), rowData.getString(1),rowData.getString(2),rowData.getString(3),rowData.getString(4));
			}
		});
		employeeDF.collect();
		//List<RuleBookColumnsPojo> listRuleBooks= ruleBookRDD.collect();
	}

}
