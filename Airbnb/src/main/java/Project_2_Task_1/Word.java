package Project_2_Task_1;

import java.util.Comparator;

import scala.Serializable;

public class Word implements Comparable<Word>, Serializable{
	
	public String word;
	public int count;
	public double tf; 
	public double idf;
	public double weight;
	
	
	public Word(String word, Integer count) {
		this.word = word;
		this.count = count;
	}

	public int compareTo(Word w) {
		if(w.weight - this.weight>0) {
			return 1;
		}
		else if (w.weight - this.weight<0) {
			return -1;
		}
		else {
			return 0;
		}
	}


}
