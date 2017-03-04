package airbnb;

import scala.Serializable;

public class AvgCount implements Serializable{
	
	public double total;
	public int num; 
	public String city;
	
	public AvgCount(String city, double total, int num) {
		this.total = total;
		this.num = num; 
		this.city = city;
	}
	
	public double avg() {
		return total / (double) num; 
	}
}
