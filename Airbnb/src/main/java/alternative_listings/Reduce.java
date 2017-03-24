package alternative_listings;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import scala.Serializable;

public class Reduce implements Serializable{
	
	public static JavaPairRDD<String, String[]> reduceByKeyOnAvailable(JavaPairRDD<String, String[]> joinedPair){


		JavaPairRDD<String, String[]> joinedPairReducedByKey = joinedPair.reduceByKey(new Function2<String[], String[], String[]>(){
			public String[] call(String[] v1, String[] v2) {
				int numberOfNotAvailable1 = Integer.parseInt(v1[v1.length-1]);
				int numberOfNotAvailable2 = Integer.parseInt(v2[v2.length-1]);
				int sum = numberOfNotAvailable1 + numberOfNotAvailable2;
				v1[v1.length-1] = ""+sum;
				return v1;
			}
		});
		//		System.out.println(joinedPair.count());
		return joinedPairReducedByKey;
	}

}
