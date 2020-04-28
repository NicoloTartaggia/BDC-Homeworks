import java.io.IOException;
import java.util.ArrayList;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class G31HW2 {

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
    public static int exactMPD(ArrayList<Vector> s) {

    }

    // 2-approximation algorithm
    public static int twoApproxMPD(ArrayList<Vector> s, int k) {

    }

    // k-center-based algorithm
    public static ArrayList<Vector> kCenterMPD(ArrayList<Vector> s, int k) {

    }

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf(true).setAppName("Homework2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Reading points from a file whose name is provided as args[0]
        String filename = args[0];
        ArrayList<Vector> inputPoints = new ArrayList<>();
        inputPoints = readVectorsSeq(filename);
    }
}
