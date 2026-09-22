package com.agrisense.p2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AvgYieldReducer
 *
 * Receives all yieldTons values for a single cropCode and computes their average.
 *
 * Input:
 *   key   → cropCode      (Text)
 *   values → [yieldTons]  (Iterable<DoubleWritable>)
 *
 * Output:
 *   key   → cropCode      (Text)
 *   value → averageYield  (DoubleWritable)
 *
 * Edge cases handled:
 *   - count == 0: no output is emitted for an empty value list (avoids divide-by-zero).
 *   - Each value is accumulated safely as a primitive double.
 */
public class AvgYieldReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable avgResult = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum   = 0.0;
        long   count = 0L;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        // Guard: skip if no valid values were received for this crop
        if (count == 0) {
            System.err.println("[WARN] No yield values received for crop: " + key.toString());
            return;
        }

        double average = sum / count;
        avgResult.set(average);
        context.write(key, avgResult);
    }
}
