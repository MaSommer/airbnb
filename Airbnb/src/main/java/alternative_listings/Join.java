package alternative_listings;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

public class Join implements Serializable{
	
	public static JavaPairRDD<String, String[]> join(JavaPairRDD<String, String[]> pair1, JavaPairRDD<String, String[]> pair2){
		JavaPairRDD<String, Tuple2<String[], String[]>> joinedPair = pair1.join(pair2);
		
		JavaPairRDD<String, String[]> joinedAndReducedPair = joinedPair
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String[],String[]>>, String, String[]>() {

					public Tuple2<String, String[]> call(
							Tuple2<String, Tuple2<String[], String[]>> t)
							throws Exception {
						String[] ret = new String[t._2._1.length + t._2._2.length-1];
//						System.out.println("1: " + Arrays.toString(t._2._1));
//						System.out.println("2: " + Arrays.toString(t._2._2));
						int index = 0;
						for (String string : t._2._1) {
							ret[index] = string;
							index++;
						}
						
						for (int i = 0; i < t._2._2.length-1; i++) {
							ret[index] = t._2._2[i];
							index++;
						}
						
						return new Tuple2<String, String[]>(t._1, ret);
					}
				});
		return joinedAndReducedPair;
	}

}
