import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class G31HW3 {

    static long SEED = 1237784;
    static Random generator = new Random(SEED);

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

    // k-center-based algorithm
    public static ArrayList<Vector> kCenterMPD(ArrayList<Vector> s, int k) {
        ArrayList<Vector> c = new ArrayList<>(); // centers
        int rand = (int) (Math.random() * s.size()); // random index for the first center selection
        c.add(s.remove(0));
        for (int i = 0; i < (k - 1); i++){ // selects a center for each iteration
            double max_dist = Double.MIN_VALUE;
            Vector maxItem = null;
            for (int j = 0; j < s.size(); j++) { // selects as center the point with maximum distance from its closest center
                double min_dist = Double.MAX_VALUE;
                for (int l = 0; l < c.size(); l++) { // d(p, S) = min{q ∈ S : d(p, q)}
                    double dist = Vectors.sqdist(c.get(l), s.get(j));
                    if (dist < min_dist)
                        min_dist = dist;
                }
                if (min_dist > max_dist) {
                    max_dist = min_dist;
                    maxItem = s.get(j);
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
        long currentTime = System.nanoTime();
        ArrayList<Vector> coreset = new ArrayList<>(pointsRDD.mapPartitions((p) -> { // R1
            ArrayList<Vector> partition = new ArrayList<>(); // centers
            while (p.hasNext()) {
                partition.add(p.next());
            }
            return kCenterMPD(partition, k).iterator();
        }).collect());
        System.out.println("Runtime of Round 1 = " + (System.nanoTime() - currentTime)/1000000);
        currentTime = System.nanoTime();
        ArrayList<Vector> twoApproxSolution = runSequential(coreset, k); // R2
        System.out.println("Runtime of Round 2 = " + (System.nanoTime() - currentTime)/1000000);
        return twoApproxSolution;
    }

    public static double measure(ArrayList<Vector> pointSet) {
        double sum = 0;
        double k = pointSet.size();
        for (int i =0; i < k; i++) {
            for (int j = i+1; j < k; j++) {
                sum += Math.sqrt(Vectors.sqdist(pointSet.get(i), pointSet.get(j)));
            }
        }
        return sum / ((k * ( k - 1 )) / 2);
    }

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

        // Average distance
        double averageDistance = measure(solution);
        System.out.println("Average distance = " + averageDistance);

    }
}
