package alternative_listings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.Function2;

import scala.Serializable;
import scala.Tuple2;
import airbnb.Reviewer;

public class Aggreagate implements Serializable{
	
	public static Function2<PriorityQueue<String[]>, Tuple2<String, String[]>, PriorityQueue<String[]>> addAndCombineTopNListings(){

		return new  Function2<PriorityQueue<String[]>, Tuple2<String, String[]>, PriorityQueue<String[]>>() {

			public PriorityQueue<String[]> call(PriorityQueue<String[]> queue, String[] v2)
					throws Exception {
				if (queue.size() < 100){
					queue.add(v2);
				}
				else{
					int inQueue = Integer.parseInt(queue.peek()[8]);
					int toCompare = Integer.parseInt(v2[8]);
					if (toCompare > inQueue){
						queue.poll();
						queue.add(v2);
					}
				}
				return queue;
			}

			public PriorityQueue<String[]> call(PriorityQueue<String[]> queue,
					Tuple2<String, String[]> v2) throws Exception {
				if (queue.size() < 100){
					queue.add(v2._2);
				}
				else{
					int inQueue = Integer.parseInt(queue.peek()[8]);
					int toCompare = Integer.parseInt(v2._2[8]);
					if (toCompare > inQueue){
						queue.poll();
						queue.add(v2._2);
					}
				}
				return queue;
			}
		};
	}
	public static Function2<ArrayList<String[]>, String[], ArrayList<String[]>> addAndCombineTopNListingsByKey(){
		
		return new  Function2<ArrayList<String[]>, String[], ArrayList<String[]>>() {
			
			public ArrayList<String[]> call(ArrayList<String[]> list, String[] v2)
					throws Exception {
				list.add(v2);				
				Collections.sort(list, new Comparator<String[]>() {
					//  0        1                2                    3          4     
					//{"id", "name", "number_of_common_amenities", "distance", "price"}
						public int compare(String[] s1, String[] s2) {
							if (Integer.parseInt(s1[2]) > Integer.parseInt(s2[2])){
								return 1;
							}
							else if (Integer.parseInt(s1[2]) < Integer.parseInt(s2[2])){
								return -1;
							}
							else{
								return 0;					
							}
						}
					});
				return list;
			}
		};
	}
	
	public static Function2<ArrayList<String[]>, ArrayList<String[]>, ArrayList<String[]>> combinePairTopNListings(final int n){
		return new Function2<ArrayList<String[]>, ArrayList<String[]>, ArrayList<String[]>>(){

			public ArrayList<String[]> call(ArrayList<String[]> list1, ArrayList<String[]> list2)
					throws Exception {
				ArrayList<String[]> ret = new ArrayList<String[]>();
				ret.addAll(list2);
				ret.addAll(list1);

				Collections.sort(ret, new Comparator<String[]>() {
					//  0        1         2            3          4           5           6           7         8         9
					//{"id", "price", "room_type", "latitude", "longitude", "amenities", "name", "available", "date", "number_of_common_amenities"}
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
				while (ret.size() > n){
					ret.remove(ret.size()-1);
				}
				return ret;
			}
		};
	}

}
