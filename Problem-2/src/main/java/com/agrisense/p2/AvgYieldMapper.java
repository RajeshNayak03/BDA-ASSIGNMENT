package com.agrisense.p2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * AvgYieldMapper
 *
 * Reads each line of yield.csv and emits:
 *   key   → cropCode  (Text)
 *   value → yieldTons (DoubleWritable)
 *
 * CSV schema: farmId, cropCode, region, season, yieldTons
 *             [0]     [1]       [2]     [3]     [4]
 *
 * Malformed or non-numeric lines are skipped and logged as warnings.
 */
public class AvgYieldMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text cropCodeKey    = new Text();
    private final DoubleWritable yieldValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip blank lines and the CSV header row
        if (line.isEmpty() || line.toLowerCase().startsWith("farmid")) {
            return;
        }

        String[] fields = line.split(",");

        // Expect exactly 5 columns
        if (fields.length != 5) {
            System.err.println("[WARN] Skipping malformed row (expected 5 fields): " + line);
            return;
        }

        String cropCode = fields[1].trim();
        if (cropCode.isEmpty()) {
            System.err.println("[WARN] Skipping row with empty cropCode: " + line);
            return;
        }

        try {
            double yieldTons = Double.parseDouble(fields[4].trim());
            cropCodeKey.set(cropCode);
            yieldValue.set(yieldTons);
            context.write(cropCodeKey, yieldValue);
        } catch (NumberFormatException e) {
            System.err.println("[WARN] Skipping row with invalid yieldTons value: " + line);
        }
    }
}
