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
				JavaRDD<String> inputRDD = sc.textFile("target/listings_us.csv");
				
				
				JavaRDD<String> result = inputRDD.map(new Function<String, String>() {
					public String call(String s) {
						System.out.println(s);
						return s;
					} 
				});
				
				System.out.println(result.count());

	}	
}
