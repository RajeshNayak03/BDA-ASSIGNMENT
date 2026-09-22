package com.agrisense.p2;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * LocalRunner — runs the Problem 2 logic locally (no Hadoop cluster needed).
 *
 * Simulates the MapReduce pipeline:
 *   Phase 1 (Map)    : reads yield.csv → emits (cropCode, yieldTons) pairs
 *   Phase 2 (Reduce) : groups by cropCode → computes average yieldTons per crop
 *   Phase 3 (Driver) : scans per-crop averages → picks the crop with the highest avg
 *
 * weather.csv is loaded and printed for reference but is not needed by Problem 2.
 *
 * Usage (from project root after mvn package):
 *   java -cp target/agrisense.jar com.agrisense.p2.LocalRunner <yieldCsv> <weatherCsv>
 *
 * Example:
 *   java -cp target/agrisense.jar com.agrisense.p2.LocalRunner yield.csv weather.csv
 */
public class LocalRunner {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: LocalRunner <yieldCsv> <weatherCsv>");
            System.exit(1);
        }

        String yieldPath   = args[0];
        String weatherPath = args[1];

        // ---------------------------------------------------------------
        // Load weather.csv (reference — not used in problem 2 computation)
        // ---------------------------------------------------------------
        System.out.println("=================================================");
        System.out.println("  AgriSense Problem 2 — Local MapReduce Simulator");
        System.out.println("=================================================");
        System.out.println();
        System.out.println("[INFO] Loading weather data from: " + weatherPath);
        List<String[]> weatherRows = loadCsv(weatherPath, "region");
        System.out.println("[INFO] Weather records loaded: " + weatherRows.size());
        System.out.println();

        // ---------------------------------------------------------------
        // PHASE 1 — MAP: read yield.csv → emit (cropCode, yieldTons)
        // ---------------------------------------------------------------
        System.out.println("[INFO] Loading yield data from: " + yieldPath);
        List<String[]> yieldRows = loadCsv(yieldPath, "farmid");
        System.out.println("[INFO] Yield records loaded: " + yieldRows.size());
        System.out.println();

        System.out.println("─── PHASE 1: MAP (cropCode → yieldTons) ─────────");
        // intermediate map: cropCode → list of yieldTons values
        Map<String, List<Double>> intermediate = new TreeMap<>();

        int skipped = 0;
        for (String[] row : yieldRows) {
            // schema: farmId[0], cropCode[1], region[2], season[3], yieldTons[4]
            if (row.length < 5) { skipped++; continue; }

            String cropCode = row[1].trim();
            String yieldStr = row[4].trim();

            if (cropCode.isEmpty()) { skipped++; continue; }

            try {
                double yieldTons = Double.parseDouble(yieldStr);
                intermediate.computeIfAbsent(cropCode, k -> new ArrayList<>()).add(yieldTons);
                System.out.printf("  emit  %-6s → %.2f%n", cropCode, yieldTons);
            } catch (NumberFormatException e) {
                System.err.println("  [WARN] Skipping bad yieldTons: " + yieldStr);
                skipped++;
            }
        }
        if (skipped > 0)
            System.out.println("  [WARN] Skipped " + skipped + " malformed row(s).");
        System.out.println();

        // ---------------------------------------------------------------
        // PHASE 2 — REDUCE: (cropCode, [yieldTons]) → (cropCode, avgYield)
        // ---------------------------------------------------------------
        System.out.println("─── PHASE 2: REDUCE (cropCode → avgYield) ────────");
        System.out.printf("  %-10s  %8s  %6s  %10s%n", "CropCode", "Sum", "Count", "Average");
        System.out.println("  " + "─".repeat(42));

        Map<String, Double> avgPerCrop = new TreeMap<>();
        for (Map.Entry<String, List<Double>> entry : intermediate.entrySet()) {
            String cropCode      = entry.getKey();
            List<Double> yields  = entry.getValue();

            double sum  = yields.stream().mapToDouble(Double::doubleValue).sum();
            long   count = yields.size();

            if (count == 0) continue; // guard: empty list (should not happen)

            double avg = sum / count;
            avgPerCrop.put(cropCode, avg);

            System.out.printf("  %-10s  %8.2f  %6d  %10.4f%n", cropCode, sum, count, avg);
        }
        System.out.println();

        // ---------------------------------------------------------------
        // PHASE 3 — DRIVER: pick the crop with the highest average yield
        // ---------------------------------------------------------------
        System.out.println("─── PHASE 3: DRIVER — Find Max Average ──────────");

        String       bestCrop = null;
        double       bestAvg  = Double.NEGATIVE_INFINITY;
        List<String> ties     = new ArrayList<>();

        for (Map.Entry<String, Double> entry : avgPerCrop.entrySet()) {
            String crop = entry.getKey();
            double avg  = entry.getValue();

            if (avg > bestAvg) {
                bestAvg = avg;
                bestCrop = crop;
                ties.clear();
                ties.add(crop);
            } else if (Double.compare(avg, bestAvg) == 0) {
                ties.add(crop);
            }
        }

        System.out.println();
        if (bestCrop == null) {
            System.err.println("[ERROR] No crop data found. Input may be empty.");
            System.exit(2);
        }

        System.out.println("=================================================");
        if (ties.size() > 1) {
            System.out.printf("  TIE: Multiple crops share highest avg yield (%.4f tons):%n", bestAvg);
            for (String c : ties) System.out.println("    → " + c);
        } else {
            System.out.printf("  Highest average yield crop: %s (%.4f tons)%n", bestCrop, bestAvg);
        }
        System.out.println("=================================================");
    }

    /**
     * Reads a CSV file, skips the header row and any row whose first field
     * (case-insensitive) matches {@code headerFirstField}.
     */
    private static List<String[]> loadCsv(String filePath, String headerFirstField) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                // Skip header row
                if (firstLine) {
                    firstLine = false;
                    if (line.toLowerCase().startsWith(headerFirstField.toLowerCase())) continue;
                }
                rows.add(line.split(","));
            }
        }
        return rows;
    }
}
