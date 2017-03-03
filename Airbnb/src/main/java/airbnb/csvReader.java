package airbnb;

import java.io.StringReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Function;

public class csvReader {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("csvReader").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
        JavaRDD<String> csvFile = sc.textFile("D:\\MOCK_DATA_spark.csv");
        JavaRDD<String[]> csvData = csvFile.map(new LoadCSVJavaExample());
        System.out.println("This prints the total count " + csvData.count());
    }

    public String[] call(String line) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(line));
        return reader.readNext();
    }
}
