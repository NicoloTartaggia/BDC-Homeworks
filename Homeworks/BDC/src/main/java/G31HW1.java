import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static java.lang.Math.max;
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

        JavaPairRDD<String, Long> deterministicCount;
        JavaPairRDD<String, Long> sparkCount;

        deterministicCount = pairStrings
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<Integer, Tuple2<String, String>> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<String, String>>> pairs = new ArrayList<>();
                    counts.put(Integer.parseInt(tokens[0]), new Tuple2<>(tokens[0], tokens[1])); // (i, (i-th obj, class)),  0 <= i < N
                    for (Map.Entry<Integer, Tuple2<String, String>> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey()%K, e.getValue())); //(i mod K, (i-th obj, class))
                    }
                    return pairs.iterator();
                })
                .groupByKey()    // <-- REDUCE PHASE (R1)
                .flatMapToPair((triplet) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Tuple2<String, String> c : triplet._2()) {
                        counts.put(c._2(), 1L + counts.getOrDefault(c._2(), 0L)); // for each 0 <= j < K gather the set S_j of all intermediate pairs with key j
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue())); // for each class labeling some object in S_j, produce the pair (class, c_j(class)),
                                                                           // where c_j(class) is the number of objects of S_j labeled with class
                    }
                    return pairs.iterator();
                })
                .groupByKey()    // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    // for each class gather the at most K pairs (class, c_j(class)) resulting at the end of the previous round and return the output pair (class, sum_j(c_j(class))
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        System.out.print("OUTPUT:\n\n" + "VERSION WITH DETERMINISTIC PARTITIONS\n" + "Output pairs = ");
        deterministicCount.sortByKey().collect().forEach(System.out::print);

        sparkCount = pairStrings
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<Integer, Tuple2<String, String>> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<String, String>>> pairs = new ArrayList<>();
                    counts.put(Integer.parseInt(tokens[0]), new Tuple2<>(tokens[0], tokens[1]));
                    for (Map.Entry<Integer, Tuple2<String, String>> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>((int) (e.getKey() % sqrt(N)), e.getValue())); // (i mod sqrt(N), (i-th obj, class))
                    }
                    return pairs.iterator();
                })
                .mapPartitionsToPair((wc) -> {    // <-- REDUCE PHASE (R1)
                    HashMap<String, Long> counts = new HashMap<>();
                    long numPairs = 0; // Counter for the number of elements in a partition
                    while (wc.hasNext()){
                        numPairs++;
                        Tuple2<Integer, Tuple2<String, String>> tuple = wc.next();
                        counts.put(tuple._2._2, 1L + counts.getOrDefault(tuple._2._2, 0L)); // for each 0 <= j < sqrt(N) gather the set S_j of all intermediate pairs with key j
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue())); // for each class labeling some object in S_j, produce the pair (class, c_j(class)),
                                                                           // where c_j(class) is the number of objects of S_j labeled with class
                    }
                    // In order to be able to compute the max partition size, we add a special pair ("maxPartitionSize", N_max)
                    pairs.add((new Tuple2<>("maxPartitionSize", numPairs)));
                    return pairs.iterator();
                })
                .groupByKey()
                .mapToPair((it) -> {
                    ArrayList<Long> acc = new ArrayList<>(); // temporary variable to handle Collections methods max() and sum()
                    it._2.forEach(acc::add);
                    if (it._1().equals("maxPartitionSize")) { // if key is "maxPartitionSize" return output pair ("maxPartitionSize", N_max)
                        return new Tuple2<>(it._1, Collections.max(acc));
                    } else { // otherwise return the output pair (class, sum_j(c_j(class))
                        return new Tuple2<>(it._1, acc.stream().mapToLong(x -> x).sum());
                    }
                });

        // For the most frequent class, ties must be broken in favor of the smaller class in alphabetical order
        System.out.println("\nVERSION WITH SPARK PARTITIONS\n" +
                "Most frequent class = " + sparkCount.sortByKey().reduce((acc, value) -> ((value._2 > acc._2) ? value : acc)) +
                "\n" + "Max partition size = " + sparkCount.filter((tuple) -> (tuple._1.equals("maxPartitionSize"))).first()._2);
    }
}
