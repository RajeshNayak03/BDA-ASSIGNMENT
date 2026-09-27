import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CorrelationReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumX = 0;
        double sumY = 0;
        double sumXY = 0;
        double sumX2 = 0;
        double sumY2 = 0;
        long n = 0;

        for (Text value : values) {

            String[] parts = value.toString().split(",");

            if (parts.length != 2) {
                continue;
            }

            double rainfall = Double.parseDouble(parts[0]);
            double yield = Double.parseDouble(parts[1]);

            sumX += rainfall;
            sumY += yield;
            sumXY += rainfall * yield;
            sumX2 += rainfall * rainfall;
            sumY2 += yield * yield;

            n++;
        }

        if (n > 1) {

            double numerator = n * sumXY - sumX * sumY;

            double denominator = Math.sqrt(
                    (n * sumX2 - sumX * sumX) *
                    (n * sumY2 - sumY * sumY)
            );

            double correlation = 0;

            if (denominator != 0) {
                correlation = numerator / denominator;
            }

            context.write(
                    key,
                    new Text(String.format("%.3f", correlation))
            );
        }
    }
}
