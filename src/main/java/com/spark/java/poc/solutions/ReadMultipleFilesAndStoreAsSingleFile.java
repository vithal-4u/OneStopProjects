package com.spark.java.poc.solutions;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.spark.java.poc.utils.SparkUtils;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class ReadMultipleFilesAndStoreAsSingleFile {
	public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
	    public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
	    	System.out.println(">>>>>>>>>>>>>>>>>>" + new StringReader(file._2()).toString());
	      CSVReader reader = new CSVReader(new StringReader(file._2()));
	      return reader.readAll().iterator();
	    }
	  }
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("ReadMultipleFilesAndStoreAsSingleFile");
		JavaPairRDD<String, String> multiFileRDD = sparkCxt
				.wholeTextFiles("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\*");
		JavaRDD<String[]> keyedRDD = multiFileRDD.flatMap(new ParseLine());
		
		keyedRDD.collect();
		//keyedRDD.saveAsTextFile("D:\\Study_Document\\Output\\Test.csv");
	}
}
