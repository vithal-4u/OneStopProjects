package com.spark.java.poc.main;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.spark.java.poc.mysqlConnect.MySQLReadQueryBased;
import com.spark.java.poc.pojo.RuleBookColumnsPojo;

public class DataValidation {

	public static void main(String[] args) {
		MySQLReadQueryBased readQueryBased = new MySQLReadQueryBased();
		JavaRDD<RuleBookColumnsPojo> ruleBookRDD = readQueryBased.getDatafromRuleBookTable("select DEFINITION from RULE_BOOK where FILE_NAME='Employee_details'");
		List<RuleBookColumnsPojo> listRuleBooks= ruleBookRDD.collect();
	}

}
