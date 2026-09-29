import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegionConsistencyReducer
        extends Reducer<Text, Text, Text, Text> {

    private String bestRegion = null;
    private double bestStdDev = Double.POSITIVE_INFINITY;

    @Override
    protected void reduce(Text region, Iterable<Text> values,
                           Context context)
            throws IOException, InterruptedException {

        Map<String, Double> seasonTotals = new HashMap<>();

        for (Text value : values) {

            String[] parts = value.toString().split("\t", -1);

            if (parts.length != 2) {
                continue;
            }

            String season = parts[0].trim();

            try {
                double yield = Double.parseDouble(parts[1].trim());

                seasonTotals.put(
                        season,
                        seasonTotals.getOrDefault(season, 0.0) + yield
                );

            } catch (NumberFormatException e) {
                // Ignore invalid values
            }
        }

        if (seasonTotals.isEmpty()) {
            return;
        }

        double sum = 0.0;

        for (double seasonalYield : seasonTotals.values()) {
            sum += seasonalYield;
        }

        double mean = sum / seasonTotals.size();

        double squaredDiffSum = 0.0;

        for (double seasonalYield : seasonTotals.values()) {
            squaredDiffSum += Math.pow(seasonalYield - mean, 2);
        }

        // Population standard deviation across the available seasons
        double variance = squaredDiffSum / seasonTotals.size();
        double stdDev = Math.sqrt(variance);

        String result = String.format(
                Locale.US,
                "mean=%.4f\tstddev=%.4f\tseasons=%d",
                mean,
                stdDev,
                seasonTotals.size()
        );

        context.write(region, new Text(result));

        // One reducer is used, so this gives the global minimum.
        if (stdDev < bestStdDev) {
            bestStdDev = stdDev;
            bestRegion = region.toString();
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        if (bestRegion != null) {

            String result = String.format(
                    Locale.US,
                    "%s\tstddev=%.4f",
                    bestRegion,
                    bestStdDev
            );

            context.write(
                    new Text("MOST_CONSISTENT_REGION"),
                    new Text(result)
            );
        }
    }
}
