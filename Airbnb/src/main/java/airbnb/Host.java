package airbnb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import scala.Serializable;

public class Host implements Serializable{
	
	private final int id;
	//listings id is key and price of listing per night is value
	private HashMap<Integer, Double> listingIDs;
	private double totalIncome;
	private int totalListings;

	private final String hostName;
	
	public Host(int id, String hostName, int listingID, int totalListings, double price){
		this.id = id;
		this.hostName = hostName;
		this.totalListings = totalListings;
		this.totalIncome = 0;
		listingIDs = new HashMap<Integer, Double>();
		listingIDs.put(listingID, price);
	}
	
	public void updateParameters(Host host){
		Iterator it = host.listingIDs.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if (!this.listingIDs.containsKey(pair.getKey())){
				this.listingIDs.put((Integer)pair.getKey(), (Double)pair.getValue());
	        }
	        it.remove();
	    }
	}
	 
	public int getId() {
		return id;
	}

	public double getTotalIncome() {
		return totalIncome;
	}

	public String getHostName() {
		return hostName;
	}
	
	public int getTotalListings() {
		return totalListings;
	}
	
	public void updateTotalIncome(Integer listingId, int numberOfNotAvialable){
		double price = listingIDs.get(listingId);
		totalIncome += price*numberOfNotAvialable;
	}
	
	public HashMap<Integer, Double> getListingIDs() {
		return listingIDs;
	}

}
