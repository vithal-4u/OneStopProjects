package com.spark.java.poc.solutions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.spark.java.poc.utils.SparkUtils;

public class IBMQuestWordCountOfEachLineDatasets {

	public static void main(String[] args) {
		Dataset<String> rowData = SparkUtils.readTextFileDataset("IBMQuestWordCountOfEachLineDatasets", "wordCount.txt");
		rowData.show(false);
		
		Dataset<String> wordTxt =rowData.flatMap(new FlatMapFunction<String, String>() {
			int i=1;
			public Iterator<String> call(String x) throws Exception {
				System.out.println("Inside dataset flatmap ---- "+i+"---"+ x.toString());
				i++;
				return  Arrays.asList(x.split(" ")).iterator();
			}
		}, Encoders.STRING());
		wordTxt.show(false);
	}

}
