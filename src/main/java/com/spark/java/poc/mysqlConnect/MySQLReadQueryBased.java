package com.spark.java.poc.mysqlConnect;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.spark.java.poc.constants.ApplicationConstants;
import com.spark.java.poc.pojo.RuleBookColumnsPojo;

public class MySQLReadQueryBased implements MySQLConnectivity,Serializable{
	
	public JavaRDD<RuleBookColumnsPojo> getDatafromRuleBookTable(String query){
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		MySQLReadQueryBased readData = new MySQLReadQueryBased();
		JavaRDD<Row> jdbcDF = readData.createConnection(ApplicationConstants.ONESTOP_SPARK_DB, query).toJavaRDD();
		
		//jdbcDF.show();
		JavaRDD<RuleBookColumnsPojo> convertedDF = jdbcDF.map(new Function<Row, RuleBookColumnsPojo>() {

			@Override
			public RuleBookColumnsPojo call(Row value) throws Exception {
				String definition = value.getString(0);
				System.out.println(definition);
				ObjectMapper mapper = new ObjectMapper();
				RuleBookColumnsPojo ruleBook=null;
				try {
					ruleBook = mapper.readValue(definition, RuleBookColumnsPojo.class);
		            System.out.print(ruleBook.getRuleBookColumns().get(0).getColumn_name());
		        } catch (Exception e) {
		            e.printStackTrace();
		        } 
				//Gson gson = new Gson();  
				//RuleBookColumnsPojo ruleBook = gson.fromJson(definition, RuleBookColumnsPojo.class);  
				//System.out.println(ruleBook.getRuleBookColumns());
				return ruleBook;
			}
		});
		//Encoder<RuleBookColumns> ruleBookEncoder = Encoders.bean(RuleBookColumns.class); 
		//Dataset<RuleBookColumns> convertedDF = jdbcDF.as(ruleBookEncoder);
		//convertedDF.show();
		return convertedDF;
	}
	
	public JavaRDD<String> getDatafromRuleBookTableStr(String query){
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		MySQLReadQueryBased readData = new MySQLReadQueryBased();
		JavaRDD<Row> jdbcDF = readData.createConnection(ApplicationConstants.ONESTOP_SPARK_DB, query).toJavaRDD();
		
		//jdbcDF.show();
		JavaRDD<String> convertedDF = jdbcDF.map(new Function<Row, String>() {

			@Override
			public String call(Row value) throws Exception {
				String definition = value.getString(0);
				
				return definition;
			}
		});
		//Encoder<RuleBookColumns> ruleBookEncoder = Encoders.bean(RuleBookColumns.class); 
		//Dataset<RuleBookColumns> convertedDF = jdbcDF.as(ruleBookEncoder);
		//convertedDF.show();
		return convertedDF;
	}
}
