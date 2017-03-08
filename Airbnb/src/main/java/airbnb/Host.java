package airbnb;

import scala.Serializable;

public class Host implements Serializable{
	
	private final int id;
	//listings id is key and price of listing per night is value
	private double totalIncome;
	private int totalListings;

	private final String hostName;
	
	public Host(int id, String hostName, int totalListings, double price, int numberOfNightsNotAvailable){
		this.id = id;
		this.hostName = hostName;
		this.totalListings = totalListings;
		this.totalIncome = price*numberOfNightsNotAvailable;
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
	
	public void updateTotalIncome(Host newHost){
		totalIncome += newHost.totalIncome;
	}

}
