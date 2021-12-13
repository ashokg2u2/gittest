package com.lrn.test;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileSum {

    private static final String COMMA_DELIMITER = ",";
    public static String SPACE_DELIMITER = " ";
    public static void main(String[] args) {

      /*  SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("numbers.txt");
        JavaRDD<String> numberStrings = input.flatMap(s -> Arrays.asList(s.split(SPACE_DELIMITER)).iterator());
        JavaRDD<String> validNumberString = numberStrings.filter(string -> !string.isEmpty());
        JavaRDD<Integer> numbers = validNumberString.map(numberString -> Integer.valueOf(numberString));
        int finalSum = numbers.reduce((x,y) -> x+y);

        System.out.println("Final sum is: " + finalSum);

        sc.close();*/

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[4]") ; // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile("nationalparks.csv");
        String header = stringJavaRDD.first() ;

        // ******** TRANSFORMATIONS ********

        JavaRDD<String> titles =stringJavaRDD.map(FileSum::extractTitle).filter(StringUtils::isNotBlank);

        /*System.out.println("Ashok titles = " + titles.count());

        JavaRDD<String> titles1 =titles.filter(StringUtils::isNotBlank);

        System.out.println("Ashok titles1 = " + titles1.count());*/
                //.filter(StringUtils::isNotBlank);
       // System.out.println("Number of lines in file = " + stringJavaRDD.count());
        JavaRDD<String> words = titles.flatMap( title -> Arrays.asList(title
                .toLowerCase()
                .trim()).iterator());
              //  .replaceAll("\\p{Punct}","")
               // .split(" ")).iterator());
        System.out.println(" words = " + words);

        // ******** COUNTING  ********


        Map<String, Long> wordCounts = words.countByValue();
        List<Map.Entry> sorted = wordCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());

        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("Number of lines in file = " + stringJavaRDD.count());

        // Test Comment
    }

    public static String extractTitle(String videoLine){
        try {
            System.out.println("videoLine  = " + videoLine);
            String temp = videoLine.split(COMMA_DELIMITER)[2];
            System.out.println("videoLine temp = " + temp);
            return temp;
        }catch (ArrayIndexOutOfBoundsException e){
            return "";
        }
    }
}
