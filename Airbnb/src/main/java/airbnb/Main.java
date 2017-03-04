package airbnb;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.StringReader;


public class Main {
	
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
        .setAppName("SF Salaries")
        .setMaster("local[*]")
        ;
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> lines = sc.text	File("target/calendar_us.csv");
		
		JavaRDD<String> inputRDD = sc.textFile("target/calendar_us.csv");
		JavaRDD<String> errorsRDD = inputRDD.filter( new Function<String, Boolean>() {
			public Boolean call(String x) { return x.contains("error"); }
		});
		errorsRDD = inputRDD.filter( new Function<String, Boolean>() {
			public Boolean call(String x) { return x.contains("error"); }
		});
		
		JavaRDD<String> 	warningsRDD = inputRDD.filter( new Function<String, Boolean>() {
			public Boolean call(String x) { return x.contains("warning"); }
		});
		JavaRDD<String> badLinesRDD = errorsRDD.union(warningsRDD);
		System.out.println("Input had " + badLinesRDD.count() + " concerning lines");
		System.out.println("Here are 10 examples:");
		for (String line: badLinesRDD.take(10)) {
		  System.out.println(line);
		}


		
//		SparkConf conf = new SparkConf()
//        .setAppName("SF Salaries")
//        .setMaster("local[*]")
//        ;
//		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(javaSparkContext);
//		System.out.println("1");
//		
//		DataFrame dataFrame = sqlContext.read()
//			    .format("com.databricks.spark.csv")
//			    .option("inferSchema", "true")
//			    .option("header", "true")
//			    .load("target/calendar_us.csv");
//		
//		System.out.println("2");
//		System.out.println(dataFrame.col("listing_id\tdate\tavailable").toString());


//	    SparkConf sparkConf = new SparkConf().setAppName("Pussy&Alcohol");
//	    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	    
//
//	    // Read the source file
//	    JavaRDD<String> input = sparkContext.textFile("target/calendar_us.csv");
//
//	    // RDD is immutable, let's create a new RDD which doesn't contain empty lines
//	    // the function needs to return true for the records to be kept
//	    JavaRDD<String> nonEmptyLines = input.filter(new Function<String, Boolean>() {
//	      public Boolean call(String s) throws Exception {
//	        if(s == null || s.trim().length() < 1) {
//	          return false;
//	        }
//	        return true;
//	      }
//	    });
//
//	    long count = nonEmptyLines.count();
//
//	    System.out.println(String.format("Total lines in %s is %d",args[0],count));
	  }
	
}
