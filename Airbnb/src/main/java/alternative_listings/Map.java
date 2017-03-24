package alternative_listings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import airbnb.HelpMethods;
import scala.Serializable;
import scala.Tuple2;

public class Map implements Serializable{

	public static JavaPairRDD<String, String[]> mapToRankByAmenities(JavaPairRDD<String, String[]> input, final String[] mainAmenitiesList){
		JavaPairRDD<String, String[]> output = input.mapToPair(new PairFunction<Tuple2<String,String[]>, String, String[]>() {
			//  0        1         2            3          4           5           6          7         8
			//{"id", "price", "room_type", "latitude", "longitude", "amenities", "name", "available", "date"}
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] ret = new String[t._2.length+1];
				int i = 0;
				for (String string : t._2) {
					ret[i] = string;
					i++;
				}

				String amenities = t._2[5];
				int counter = 0;
				if (amenities.length() > 3){
					String amenitiesReduced = amenities.substring(2, amenities.length()-2);
					String[] amenitiesList = amenitiesReduced.split(",");
					for (String listingAmenity : amenitiesList) {
						for (String mainAmenity : mainAmenitiesList) {
							if (listingAmenity.equals(mainAmenity)){
								counter++;
							}
						}
					}
					ret[ret.length-1] = "" + counter;					
				}
				else{
					ret[ret.length-1] = "0";
				}
				//returns:
				//  0        1         2            3          4           5             6        7           8           9
				//{"id", "price", "room_type", "latitude", "longitude", "amenities", ""name", "available", "date", "number_of_common_amenities"}
				return new Tuple2<String, String[]>(counter+"", ret);
			}
		});

		return output;
	}


	public static JavaPairRDD<String, String[]> mapToCommonKey(JavaPairRDD<String, String[]> input, final String mainLat, final String mainLong){
		JavaPairRDD<String, String[]> output = input.mapToPair(new PairFunction<Tuple2<String,String[]>, String, String[]>() {
			//  0        1         2            3          4           5             6         7        8            9
			//{"id", "price", "room_type", "latitude", "longitude", "amenities", "name", "available", "date", "number_of_common_amenities"}
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] ret = new String[5];
				ret[0] = t._2[0];
				ret[1] = t._2[6];
				ret[2] = t._2[t._2.length-1];
				ret[3] = "" + HelpMethods.distance(mainLat, mainLong, t._2[3], t._2[4]);
				ret[4] = t._2[1];

				return new Tuple2<String, String[]>("1", ret);
			}
		});

		return output;
	}

	public static JavaPairRDD<String, String[]> mapNumberOfCommonAmenitiesAsKey(JavaPairRDD<String, String[]> input){
		JavaPairRDD<String, String[]> output = input.mapToPair(new PairFunction<Tuple2<String,String[]>, String, String[]>() {
			//  0        1         2            3          4           5         6           7           8         9
			//{"id", "price", "room_type", "latitude", "longitude", "amenities", "name", "available", "date", "number_of_common_amenities"}
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] ret = t._2;
				return new Tuple2<String, String[]>(t._2[t._2.length-1], ret);
			}
		});

		return output;
	}

	public static JavaRDD<String> mapToStringPrintQueue(JavaPairRDD<String, ArrayList<String[]>> input){
		JavaRDD<String> outputPrint = input.map(new Function<Tuple2<String,ArrayList<String[]>>, String>() {


			public String call(Tuple2<String, ArrayList<String[]>> v1)
					throws Exception {
				Collections.sort(v1._2, new Comparator<String[]>() {
					public int compare(String[] s1, String[] s2) {
						if (Integer.parseInt(s1[2]) < Integer.parseInt(s2[2])){
							return 1;
						}
						else if (Integer.parseInt(s1[2]) > Integer.parseInt(s2[2])){
							return -1;
						}
						else{
							return 0;					
						}
					}
				});
				ArrayList<String[]> list = v1._2;
				String ret = "";
				String header = "";
				for (String[] row : list) {
					for (String att : row) {
						ret += att + "\t";							
					}
					ret += "\n";
				}
				return ret;
			}
		});
		return outputPrint;
	}
	
	public static JavaRDD<String> mapToStringVisualisation(JavaPairRDD<String, String[]> input){
		JavaRDD<String> output = input.map(new Function<Tuple2<String,String[]>, String>() {
			//   0       1         2           3             4          5           6            7                   8               9        10         11
			// "id", "price", "room_type", "latitude", "longitude", "amenities", "name", "review_scores_rating", "picture_url", "available, "date, numberOfCommonAmenities
			public String call(Tuple2<String, String[]> v1) throws Exception {
				String ret = "";
				for (int i = 0; i < v1._2.length; i++) {
					if (i == 5 || i == 9 || i == 10){
						continue;
					}
					else{
						if (i != 11){
							String s = v1._2[i];
							for (int j = 0; j < s.length(); j++) {
								if (s.charAt(j) != ','){
									ret += s.charAt(j);
								}
							}
							ret += ",";
						} 
						else{
							String s = v1._2[i];
							for (int j = 0; j < s.length(); j++) {
								if (s.charAt(j) != ','){
									ret += s.charAt(j);
								}
							}
						}
					}
				}
				
				return ret;
			}
		});
		return output;
	}


}
