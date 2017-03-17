package airbnb;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


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
		
	}
	
	public static void main(String[] args) {
		new Main();
		SparkConf conf = new SparkConf()
				.setAppName("SF Salaries")
				.setMaster("local[*]")
				;
				JavaSparkContext sc = new JavaSparkContext(conf);
				JavaRDD<String> inputRDD = sc.textFile("target/listings_us.csv");
				
				JavaRDD<String[]> result = inputRDD.map(new Function<String, String[]>() {
					public String[] call(String s) {
//						System.out.println(s);
						String[] cityAndPrice = new String[2];
						String[] list = s.split("\t");
						cityAndPrice[0] = list[15];
						cityAndPrice[1] = list[65];
						
						return cityAndPrice;
					} 
				});
				
				System.out.println(result.reduce(new Function2<String[],String[], String[]>() {
						public String[] call(String[] arg0, String[] arg1) throws Exception {
						
						return null;
					}
					
				}));
			}
	}	
