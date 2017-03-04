package airbnb;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import java.io.StringReader;
import java.util.Arrays;


public class Main {	

	public int priceIndex;

	public Main() {
		
//		int priceIndex = -1;
//		int count = 0;
//		String[] infoList = inputRDD.first().split("\t");
//		for (String string : infoList) {
//			if (string.equals("city")){
//				priceIndex = count;
//				break;
//			}
//			count++;
//		}
//		System.out.println(priceIndex);
//		ArrayList<String> priceList = new ArrayList<String>();
		

		SparkConf conf = new SparkConf()
		.setAppName("SF Salaries")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputRDD = sc.textFile("target/listings_us.csv");
		
		JavaRDD<String[]> result = inputRDD.map(new Function<String, String[]>() {
			public String[] call(String s) {
//				System.out.println(s);
				String[] cityAndPrice = new String[2];
				String[] list = s.split("\t");
				cityAndPrice[0] = list[15];
				cityAndPrice[1] = list[65];
				System.out.println(Arrays.toString(cityAndPrice));
				
				return cityAndPrice;
			} 
		});
		
		System.out.println(result.count());
		

	}
	
	public static void main(String[] args) {
		new Main();
	}	
}
