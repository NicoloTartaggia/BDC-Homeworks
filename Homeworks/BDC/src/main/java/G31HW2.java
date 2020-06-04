import java.io.IOException;
import java.util.ArrayList;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

// - Program must run correctly for pointsets belonging to Euclidean spaces of any dimensionality.
//   Even if the datasets that we provided to test the program are in 2 dimensions, the program must work as well for datasets in R^d for arbitrary d.
// - Running times of all methods must be expressed in milliseconds.
public class G31HW2 {

    private static final long SEED = 1237784;

    // Auxiliary methods
    public static Vector strToVector (String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(str -> strToVector(str))
                .forEach(e -> result.add(e));
        return result;
    }

    // Exact algorithm
    public static double exactMPD(ArrayList<Vector> s) {
        double maxDistance = 0;
        for (Vector v1 : s) {
            for (Vector v2 : s) {
                double currentDistance = Math.sqrt(Vectors.sqdist(v1, v2));
                if (currentDistance > maxDistance) {
                    maxDistance = currentDistance;
                }
            }
        }
        return maxDistance;
    }

    // 2-approximation algorithm
    public static double twoApproxMPD(ArrayList<Vector> s, int k) {
        Random rand = new Random(); // Initialize random object
        rand.setSeed(SEED); // Set its seed
        ArrayList<Vector> s1 = new ArrayList<>();
        for (int i = 0; i<k; i = i+1) {
            s1.add(s.remove(rand.nextInt(s.size())));  // Adding k random point from s to s1
        }
        double maxDistance = 0;
        for (Vector v1 : s1) {
            for (Vector v2 : s) {
                double currentDistance = Math.sqrt(Vectors.sqdist(v1, v2));
                if (currentDistance > maxDistance) {
                    maxDistance = currentDistance;
                }
            }
        }
        return maxDistance;
    }

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

    public static void main(String[] args) throws IOException {
        // Reading points from a file whose name is provided as args[0]
        String filename = args[0];
        int k = Integer.parseInt(args[1]);
        ArrayList<Vector> inputPoints = readVectorsSeq(filename);

        // Exact algorithm output
        System.out.println("EXACT ALGORITHM");
        long startTime1 = System.nanoTime();
        double exactMaxDistance1 = exactMPD(inputPoints);
        long estimatedTime1 = System.nanoTime() - startTime1;
        System.out.println("Max distance = " + exactMaxDistance1 +
                               "\nRunning time = " + (estimatedTime1)/1000000 + "\n");

        // 2-approximation algorithm output
        System.out.println("2-APPROXIMATION ALGORITHM");
        long startTime2 = System.nanoTime();
        double approxMaxDistance = twoApproxMPD(inputPoints, k);
        long estimatedTime2 = System.nanoTime() - startTime2;
        System.out.println("k = " + k +
                           "\nMax distance = " + approxMaxDistance +
                           "\nRunning time = " + (estimatedTime2)/1000000 + "\n");

        // k-center-based algorithm output
        System.out.println("k-CENTER-BASED ALGORITHM");
        long startTime3 = System.nanoTime();
        ArrayList<Vector> centers = kCenterMPD(inputPoints, k);
        double exactMaxDistance2 = exactMPD(centers);
        long estimatedTime3 = System.nanoTime() - startTime3;
        System.out.println("k = " + k +
                          "\nMax distance = " + exactMaxDistance2 +
                          "\nRunning time = " + (estimatedTime3)/1000000 + "\n");
    }
}
