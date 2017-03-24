package airbnb;

import scala.Serializable;

public class Host implements Serializable{
	
	private final int id;
	//listings id is key and price of listing per night is value
	private double totalIncome;

	private final String hostName;
	
	public Host(int id, String hostName, double totalIncome){
		this.id = id;
		this.hostName = hostName;
		this.totalIncome = totalIncome;
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

	public void updateTotalIncome(Host newHost){
		totalIncome += newHost.totalIncome;
	}

}
