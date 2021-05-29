package com.spark.java.poc.aws.s3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Array;
import com.spark.java.poc.utils.SparkUtils;

/**
 * This class will read data from AWS S3 and prints to Eclipse console.
 * 
 * @author kasho
 *
 */
public class ReadDataFromS3 {

	public static void main(String[] args) {
		
		SparkSession spark = SparkUtils.createSparkSession("Read data from S3");
		spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "<AWS-Access-Key>");
		spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "<AWS-Secret-Key>");
		
		Dataset<Row> awsFileDataset = spark.read()
				//.option("header", "false")
                .text("s3a://data-storage-v1/airports.txt");
		awsFileDataset.show(false);
	}

}
