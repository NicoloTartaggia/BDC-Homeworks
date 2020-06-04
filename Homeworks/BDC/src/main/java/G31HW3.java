import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class G31HW3 {

    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {

        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter=0; iter<k/2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i+1; j < n; j++) {
                        if (candidates[j]) {
                            // Use squared euclidean distance to avoid an sqrt computation!
                            double d = Vectors.sqdist(points.get(i), points.get(j));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;

    } // END runSequential

    // best k-clustering around these centers is the one where each point is assigned to the cluster of the closest center
    // p: {pointset - s}, s: k centers
    public static ArrayList<ArrayList<Vector>> Partition (ArrayList<Vector> p, ArrayList<Vector> s) {
        ArrayList<ArrayList<Vector>> partitions = new ArrayList<>();  // each ArrayList<Vector> is a partition e contains its point
        for (Vector center : s) { // initialize the partitions
            ArrayList<Vector> partition = new ArrayList<>();
            partition.add(center); // add the center as first element of the partition
            partitions.add(partition);
            p.remove(center); // remove the center from all points
        }
        for (Vector point : p) { // assign the points to the correct partition
            double minDistance = Double.POSITIVE_INFINITY;
            int centerIndex = 0;
            for (int j = 0; j < s.size(); j++) {  // find the point with minimum distance
                double currentDistance = Math.sqrt(Vectors.sqdist(point, s.get(j)));
                if (currentDistance < minDistance) {
                    minDistance = currentDistance;
                    centerIndex = j;
                }
            }
            partitions.get(centerIndex).add(point); // assing the point
        }
        return partitions;
    }

    // k-center-based algorithm
    public static ArrayList<Vector> kCenterMPD(ArrayList<Vector> s, int k) {
        ArrayList<Vector> c = new ArrayList<>(); // centers
        int rand = (int) (Math.random() * s.size()); // random index for the first center selection
        c.add(s.remove(rand));
        for (int i = 0; i < (k - 1); i++){ // selects a center for each iteration
            ArrayList<ArrayList<Vector>> currentPartitions = Partition(s, c); // assign the input points to the partitions
            double maxDistance = 0;
            Vector maxItem = null;
            for (int l = 0; l < currentPartitions.size(); l++) { // find the center which is the point with max distance from its closest center
                for (int j = 0; j < currentPartitions.get(l).size(); j++) {
                    double currentDistance = Math.sqrt(Vectors.sqdist(c.get(l), currentPartitions.get(l).get(j)));
                    if (currentDistance > maxDistance) {
                        maxDistance = currentDistance;
                        maxItem = currentPartitions.get(l).get(j);
                    }
                }
            }
            s.remove(maxItem); // remove the center from s
            c.add(maxItem); // add the new center to c
        }
        return c;
    }

    // f creates a Vector from a string representing its coordinates
    public static Vector f(String point){
        return Vectors.dense(Arrays.stream(point.split(",")).mapToDouble(Double::parseDouble).toArray());
    }

    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsRDD, int k, int L) {
        ArrayList<Vector> x = new ArrayList<>();
        pointsRDD.mapPartitions((p) -> {
            ArrayList<Vector> partition = new ArrayList<>(); // centers
            while (p.hasNext()) {
                //System.out.println(p.next());
                partition.add(p.next());
            }
            System.out.println("Stop partition");
            return kCenterMPD(partition, k).iterator();
        }).collect();
        return x;
    }

    //public static int measure(ArrayList<Vector> pointsSet) {}

    public static void main(String[] args) throws IOException {

        /* Initalizing Spark context */
        long initializationTime = System.nanoTime();
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: file_path diversity_maximization_param num_partitions");
        }

        SparkConf conf = new SparkConf(true).setAppName("Homework3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        String inputPath = args[0];            // Read the input path
        int k = Integer.parseInt(args[1]);     // Read the integer k - parameter for diversity maximization
        int L = Integer.parseInt(args[2]);     // Read the integer L - number of partitions
        long N = sc.textFile(args[0]).count(); // Read number of points

        // Read input file and subdivide it into L random partitions
        JavaRDD<Vector> inputPoints = sc.textFile(inputPath).map(G31HW3::f).repartition(L).cache();

        System.out.println("Number of points = " + N +
                           "\nk = " + k +
                           "\nL = " + L +
                           "\nInitialization time = " + (System.nanoTime() - initializationTime)/1000000);

        // runMapReduce solution
        ArrayList<Vector> solution = runMapReduce(inputPoints, k, L);
        System.out.println("Runtime of Round 1 = " +
                           "Runtime of Round 2 = ");

        // Average distance
        //int averageDistance = measure(solution);
        //System.out.println("Average distance = " + averageDistance);

    }
}
