import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegionConsistencyReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        Map<String, Double> seasonTotals = new HashMap<>();

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String season = parts[0];
            double yield = Double.parseDouble(parts[1]);

            seasonTotals.put(season,
                    seasonTotals.getOrDefault(season, 0.0) + yield);
        }

        double sum = 0.0;

        for (double total : seasonTotals.values()) {
            sum += total;
        }

        double mean = sum / seasonTotals.size();

        double squaredDifference = 0.0;

        for (double total : seasonTotals.values()) {
            squaredDifference += Math.pow(total - mean, 2);
        }

        double standardDeviation =
                Math.sqrt(squaredDifference / seasonTotals.size());

        context.write(key,
                new Text(String.format("%.2f", standardDeviation)));
    }
}
