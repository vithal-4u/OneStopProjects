package com.spark.java.poc.main;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class JoinTwoDatasets {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		SparkSession sparkSess = SparkUtils.createSparkSession("JoinTwoDatasets");
		Dataset<Row> empFileData = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\employees.csv");
		Dataset<Row> salFileData = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\salaries.csv");
		//empFileData.show(10);
		//salFileData.show(10);
		
		empFileData.createOrReplaceTempView("Employee");
		salFileData.createOrReplaceTempView("Salary");
		
		//First way to Join
		//empFileData.join(salFileData, empFileData.col("emp_no").equalTo(salFileData.col("emp_no"))).show(30);
		
		empFileData.join(salFileData).where("Employee.emp_no = Salary.emp_no").show(50);
		//Second way to Join
		//sparkSess.sql("Select Employee.emp_no,Employee.first_name,Employee.last_name,Employee.birth_date,Employee.gender,Employee.hire_date,Salary.salary,Salary.from_date,Salary.to_date from Employee, Salary where Employee.emp_no = Salary.emp_no").show(40);
		//JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("JoinTwoDatasets");
		//JavaRDD<String> linesRDD = sparkCxt.textFile("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\employees.csv");
	}

}
