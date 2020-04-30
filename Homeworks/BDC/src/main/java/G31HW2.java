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
                // double[] p1 = v1.toArray();
                // double[] p2 = v2.toArray();
                // double currentDistance = Math.sqrt(Math.pow((p1[0] - p2[0]), 2) + Math.pow((p1[1] - p2[1]), 2));
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
            s1.add(s.get(rand.nextInt(s.size())));  // Adding k random point from s to s1
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

    // k-center-based algorithm
    //public static ArrayList<Vector> kCenterMPD(ArrayList<Vector> s, int k) {

    //}

    public static void main(String[] args) throws IOException {
        // Reading points from a file whose name is provided as args[0]
        String filename = args[0];
        int k = Integer.parseInt(args[1]);
        ArrayList<Vector> inputPoints = new ArrayList<>();
        inputPoints = readVectorsSeq(filename); // inputPoints = [[x1,y1], [x2,y2],..., [xn,yn]]

        // Exact algorithm output
        System.out.println("EXACT ALGORITHM");
        long startTime1 = System.currentTimeMillis();
        double exactMaxDistance1 = exactMPD(inputPoints);
        System.out.println("Max distance = " + exactMaxDistance1 +
                           "\nRunning time = " + (System.currentTimeMillis() - startTime1) + "\n");

        // 2-approximation algorithm output
        System.out.println("2-APPROXIMATION ALGORITHM");
        long startTime2 = System.currentTimeMillis();
        double approxMaxDistance = twoApproxMPD(inputPoints, k);
        System.out.println("k = " + k +
                           "\nMax distance = " + approxMaxDistance +
                           "\nRunning time = " + (System.currentTimeMillis() - startTime2) + "\n");

        //// k-center-based algorithm output
        //System.out.println("k-CENTER-BASED ALGORITHM");
        //long startTime3 = System.currentTimeMillis();
        //ArrayList<Vector> centers = kCenterMPD(inputPoints, k);
        //int exactMaxDistance2 = exactMPD(centers);
        //System.out.println("k = " + k +
        //                   "\nMax distance = " + exactMaxDistance2 +
        //                   "\nRunning time = " + (System.currentTimeMillis() - startTime3) + "\n");
    }
}
