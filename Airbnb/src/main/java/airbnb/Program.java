package airbnb;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Serializable;
import scala.Tuple2;



public class Program {	

	private static JavaRDD<String> listings_usRDD;
	private static JavaRDD<String> reviews_usRDD;
	private static JavaRDD<String> calendar_usRDD;
	public static int cityIndex;

	public Program(JavaSparkContext sc){
		listings_usRDD = sc.textFile("target/listings_us.csv");
		reviews_usRDD = sc.textFile("target/reviews_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
//		task3();
	}

	
	public static void task2b(){
		
		HashMap<String,Integer> result = new HashMap<String, Integer>(); 
				
		//b) Calculate number of distinct values for each field
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] headerList = listings_usRDD.first().split("\t");
		
		for (int i = 0; i < headerList.length; i++) {
			String col = headerList[i];
			
			try {
				JavaRDD<String> ret = HelpMethods.mapToColumnsString(listings_usRDD, col, 'l').distinct();				
				int num = (int) ret.count();
				result.put(col,num );
			} 
			catch(Exception e) {
				continue;
			}
		}
		
		List<String> keys = new ArrayList(result.keySet());
		Collections.sort(keys);
		
		for (String s : keys) {
			System.out.println(s + " has: " + result.get(s) + " distinct fields");
		}
		
	}

	//c) Listings from how many and which cities are contained in the dataset
	public static void task2c(){
	 
		
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] headerList = listings_usRDD.first().split("\t");
		 
		
		for (int i = 0; i < headerList.length; i++) {
			if (headerList[i].equals("city")) {
				cityIndex = i;
			}
		}
		
		try {
			JavaRDD<String> ret = HelpMethods.mapToColumnsString(listings_usRDD, headerList[cityIndex], 'l').distinct();				
			int num = (int) ret.count();
			ret.foreach(new VoidFunction<String>() {

				public void call(String t) throws Exception {
					if(t.equals("city")) {
						
					}
					else {
						System.out.println(t);						
					}
				}
				
			});
			System.out.println(num-1);
		} 
		catch(Exception e) {
			System.out.println("Unable to run");
		}
		
}
	
	
	
	


	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);
		Program p=new Program(sc);
		p.task2c();
	}


}
