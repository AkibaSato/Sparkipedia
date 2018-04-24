package spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class SimpleApp {
  public static void sparkyTheLongLostSpark(String[] args) {
//    String logFile = "TestFile.txt"; // Should be some file on your system
//    SparkSession spark = SparkSession.builder().appName("spark.SimpleApp").config("spark.master", "local").getOrCreate();
//    Dataset<String> logData = spark.read().textFile(logFile).cache();
//
//    long numAs = logData.filter(s -> s.contains("a")).count();
//    long numBs = logData.filter(s -> s.contains("b")).count();
//
//    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//
//    spark.stop();
    
    
    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

	  SparkSession session = SparkSession.builder().master("local").appName("SimpleApp").getOrCreate();
	  Dataset<Row> list = session.read().json("/Users/brad/Desktop/Brandeis2017/Sparkipedia/src/main/resources");
	  list.select("apple").show();
    
    //JavaRDD <String>lines = sc.textFile("output/part-r-00000")
    //	    .filter(line -> line.length() > 10)
	//    .filter(line -> line.indexOf("penguin") != -1)
	//    .map(line -> line.toUpperCase());
	//List<String> outputs = lines.collect();
	//for (String output : outputs) {
	//	System.out.println(output);
	//}

	sc.close();

  }
}