package airbnb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.StringReader;

public class Main {
	
	
	public static void main(String[] args) {
<<<<<<< HEAD
		System.out.println("Heisann pussy");
	}
=======
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
	

>>>>>>> Marty
}
