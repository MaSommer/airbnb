package airbnb;

import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Print {
	
	public static DecimalFormat numberFormat = new DecimalFormat("#.00");

	
	public static void task3a(JavaPairRDD<String, String[]> mappedListingsPairAggregated){
		System.out.println("Avg booking price per night:");
		mappedListingsPairAggregated.foreach(new VoidFunction<Tuple2<String, String[]>>(){
			public void call(Tuple2<String, String[]> t) throws Exception {
				double avg = HelpMethods.stringToDouble(t._2[1])/HelpMethods.stringToDouble(t._2[2]);
				System.out.println(t._1 + ":\t$" + numberFormat.format(avg));
			}
		});
	}
	
	public static void task3b(JavaPairRDD<String, String[]> mappedListingsPairAggregated){
		System.out.println("Avg booking price per room type per night:");
		mappedListingsPairAggregated.foreach(new VoidFunction<Tuple2<String, String[]>>(){
			public void call(Tuple2<String, String[]> t) throws Exception {
				double avg = HelpMethods.stringToDouble(t._2[1])/HelpMethods.stringToDouble(t._2[2]);
				System.out.println(t._1 + ":\t$" + numberFormat.format(avg));
			}
		});
	}
	
	public static void task3c(JavaPairRDD<String, String[]> mappedListingsPairAggregated){
		System.out.println("Avg number of reviews per month:");
		mappedListingsPairAggregated.foreach(new VoidFunction<Tuple2<String, String[]>>(){
			public void call(Tuple2<String, String[]> t) throws Exception {
				double avg = HelpMethods.stringToDouble(t._2[1])/HelpMethods.stringToDouble(t._2[2]);
				System.out.println(t._1 + ":\t" + numberFormat.format(avg));
			}
		});
	}
	
	public static void task3d(JavaPairRDD<String, String[]> mappedListingsPairAggregated){
		System.out.println("Estimated number of nights booked per year:");
		mappedListingsPairAggregated.foreach(new VoidFunction<Tuple2<String, String[]>>(){
			public void call(Tuple2<String, String[]> t) throws Exception {
				System.out.println(t._1 + ":\t" + numberFormat.format(Double.parseDouble(t._2[1])));
			}
		});
	}
	
	public static void task3e(JavaPairRDD<String, String[]> mappedListingsPairAggregated){
		System.out.println("Estimated amount of money spent on AirBnB accomodation per year:");
		mappedListingsPairAggregated.foreach(new VoidFunction<Tuple2<String, String[]>>(){
			public void call(Tuple2<String, String[]> t) throws Exception {
				System.out.println(t._1 + ":\t$" + numberFormat.format(Double.parseDouble(t._2[1])));
			}
		});
	}
	
	public static void task4ab(double[] ret){
		System.out.println("Global avg number of listings per host: " + numberFormat.format(ret[0]/ret[1]));
		System.out.println("Percentage of hosts with more than 1 listing: " + numberFormat.format(100*ret[2]/ret[1])+"%");
	}
	
	public static void task4c(JavaPairRDD<String, ArrayList<Host>> mappedListingsPairAggregated){		
		mappedListingsPairAggregated.foreach(new VoidFunction<Tuple2<String, ArrayList<Host>>>(){
			public void call(Tuple2<String, ArrayList<Host>> t)
					throws Exception {
				System.out.println("Top 3 hosts with the highsest income for city " + t._1+":");
				int rank = 1;
				for (Host host : t._2) {
					System.out.println(rank +". Name: " + host.getHostName() + "\tID: " + host.getId() + "\t Income: $" + host.getTotalIncome());
					rank++;
				}
				
			}
		});
	}

}
