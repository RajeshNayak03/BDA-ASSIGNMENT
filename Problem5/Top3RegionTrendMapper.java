import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top3RegionTrendMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip CSV header
        if (line.startsWith("farmId,")) {
            return;
        }

        String[] parts = line.split(",", -1);

        if (parts.length != 5) {
            return;
        }

        String region = parts[2].trim();
        String season = parts[3].trim();

        try {
            double yield = Double.parseDouble(parts[4].trim());

            outKey.set(region);
            outValue.set(season + "\t" + yield);

            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // Ignore invalid yield values
        }
    }
}
