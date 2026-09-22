package com.agrisense.p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * AvgYieldDriver
 *
 * Orchestrates the two-step solution for Problem 2:
 *
 *   Step 1 — MapReduce Job:
 *     Computes the average yieldTons per cropCode and writes results to HDFS.
 *
 *   Step 2 — Driver-side scan (Option A):
 *     Reads the tiny per-crop output via the HDFS FileSystem API and picks
 *     the crop with the highest average yield. Prints the winner to stdout.
 *
 * Usage:
 *   hadoop jar agrisense.jar com.agrisense.p2.AvgYieldDriver \
 *       /agrisense/input/yield.csv  /agrisense/output/p2_avg
 *
 * Arguments:
 *   args[0] → input path  (yield.csv on HDFS)
 *   args[1] → output path (directory for per-crop averages)
 */
public class AvgYieldDriver {

    // Name of the part file produced by a single-reducer job
    private static final String PART_FILE = "part-r-00000";

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: AvgYieldDriver <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // ---------------------------------------------------------------
        // Step 1: MapReduce job — average yield per crop
        // ---------------------------------------------------------------
        Job job = Job.getInstance(conf, "AgriSense-P2-AverageYieldPerCrop");
        job.setJarByClass(AvgYieldDriver.class);

        job.setMapperClass(AvgYieldMapper.class);
        job.setReducerClass(AvgYieldReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Reducer / final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Use a single reducer so output lands in exactly one part file
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (!success) {
            System.err.println("[ERROR] Job 1 (AverageYieldPerCrop) failed. Exiting.");
            System.exit(2);
        }

        // ---------------------------------------------------------------
        // Step 2: Driver-side scan — find the crop with the highest average
        // ---------------------------------------------------------------
        Path outputFile = new Path(args[1] + "/" + PART_FILE);
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(outputFile)) {
            System.err.println("[ERROR] Output file not found: " + outputFile);
            System.exit(3);
        }

        String bestCrop  = null;
        double bestAvg   = Double.NEGATIVE_INFINITY;

        // Track ties: multiple crops sharing the exact same max average
        java.util.List<String> tiedCrops = new java.util.ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(outputFile)))) {

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Job 1 output is tab-separated: cropCode \t avgYield
                String[] parts = line.split("\t");
                if (parts.length != 2) {
                    System.err.println("[WARN] Unexpected output format, skipping: " + line);
                    continue;
                }

                String crop = parts[0].trim();
                double avg;
                try {
                    avg = Double.parseDouble(parts[1].trim());
                } catch (NumberFormatException e) {
                    System.err.println("[WARN] Cannot parse average for crop '" + crop + "': " + parts[1]);
                    continue;
                }

                if (avg > bestAvg) {
                    bestAvg = avg;
                    bestCrop = crop;
                    tiedCrops.clear();
                    tiedCrops.add(crop);
                } else if (avg == bestAvg) {
                    // Tie: record all crops sharing the maximum
                    tiedCrops.add(crop);
                }
            }
        }

        // ---------------------------------------------------------------
        // Report result
        // ---------------------------------------------------------------
        if (bestCrop == null) {
            System.err.println("[ERROR] No crop data found in output. Input may be empty.");
            System.exit(4);
        }

        if (tiedCrops.size() > 1) {
            // Tie scenario
            System.out.println("Tie detected! Multiple crops share the highest average yield ("
                    + String.format("%.4f", bestAvg) + " tons):");
            for (String c : tiedCrops) {
                System.out.println("  - " + c);
            }
        } else {
            // Clear winner
            System.out.println("Highest average yield crop: "
                    + bestCrop + " (" + String.format("%.4f", bestAvg) + " tons)");
        }
    }
}
