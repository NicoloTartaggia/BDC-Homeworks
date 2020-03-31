import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static java.lang.Math.sqrt;

public class G31HW1 {
    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        SparkConf conf = new SparkConf(true).setAppName("Homework1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        int K = Integer.parseInt(args[0]);
        long N = sc.textFile(args[1]).count();

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> pairStrings = sc.textFile(args[1]).repartition(K);

        System.out.println("INPUT:\n\n" + "** K=" + K + "\n\n" + "** DATASET: " + sc.textFile(args[1]).name() + "\n");

        JavaPairRDD<String, Long> count;
        JavaPairRDD<String, Long> count1;

        count = pairStrings
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1) - Transform each document into a set of
                                                  //     key-value pairs
                    String[] tokens = document.split(" ");
                    HashMap<Integer, Tuple2<String, String>> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<String, String>>> pairs = new ArrayList<>();
                    counts.put(Integer.parseInt(tokens[0]), new Tuple2<>(tokens[0], tokens[1]));
                    for (Map.Entry<Integer, Tuple2<String, String>> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey()%K, e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey()    // <-- REDUCE PHASE (R1)
                .flatMapToPair((triplet) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Tuple2<String, String> c : triplet._2()) {
                        counts.put(c._2(), 1L + counts.getOrDefault(c._2(), 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey()    // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        System.out.println("OUTPUT:\n\n" + "VERSION WITH DETERMINISTIC PARTITIONS\n" + "Output pairs = ");

        count.sortByKey().collect().forEach(System.out::println);

        count1 = pairStrings
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<Integer, Tuple2<String, String>> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<String, String>>> pairs = new ArrayList<>();
                    counts.put(Integer.parseInt(tokens[0]), new Tuple2<>(tokens[0], tokens[1]));
                    for (Map.Entry<Integer, Tuple2<String, String>> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>((int) (e.getKey() % sqrt(N)), e.getValue())); //(i mod sqrt(N), (obj, class))
                    }
                    return pairs.iterator();
                })
                .mapPartitionsToPair((wc) -> {    // <-- REDUCE PHASE (R1)
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    int i = 0; //counts the number of elements in a partition
                    while (wc.hasNext()){
                        i++;
                        Tuple2<Integer, Tuple2<String, String>> tuple = wc.next();
                        counts.put(tuple._2._2, 1L + counts.getOrDefault(tuple._2._2, 0L));
                    }

                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey()     // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        String mostFreqClass = count1.collect().stream().max(Comparator.comparing(Tuple2::_2)).get().toString();
        System.out.println("VERSION WITH SPARK PARTITIONS\n" + "Most frequent class = " + mostFreqClass + "\n" + "Max partition size = ");

        //TODO :: for the most frequent class, ties must be broken in favor of the smaller class in alphabetical order)
        /*Comparator<Tuple2<String, Integer>> myComparator = new Comparator<Tuple2<String, Integer>>() {
            public int compare(Tuple2 t1, Tuple2 t2) {
                if(t1._2 > t2._2)
                    return 1;
                else if (t1._2 == t2._2){
                    if (t1._1 >= t2._1)
                        return 1;
                    else
                        return -1;
                }
                else return -1;
            }
        };

        ArrayList<Tuple2<String, Integer>> test = new ArrayList<>();
        test.add(new Tuple2<String, Integer>("Zzzz", 3055));
        test.add(new Tuple2<String, Integer>("Horror", 3055));
        test.forEach(System.out::println);
        String mostFreqClassTest = test.stream().max(Comparator.comparingInt(T)).get().toString();
        System.out.println(mostFreqClassTest);
        */
    }
}
