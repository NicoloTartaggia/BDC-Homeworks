import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class G31HW1 {
    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        SparkConf conf = new SparkConf(true).setAppName("Homework1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        int K = Integer.parseInt(args[0]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> pairStrings = sc.textFile(args[1]).repartition(K);

        System.out.println("INPUT:\n\n" + "** K=" + K + "\n\n" + "** DATASET: " + sc.textFile(args[1]).name() + "\n");

        JavaPairRDD<String, Long> count;

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

        count.sortByKey().collect().forEach(s -> System.out.println(s));

        System.out.println("VERSION WITH SPARK PARTITIONS\n" + "Most frequent class = " + "\n" + "Max partition size = ");
    }
}
