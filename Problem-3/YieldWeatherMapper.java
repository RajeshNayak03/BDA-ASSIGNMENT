import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class YieldWeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.startsWith("farmId") || line.startsWith("region,season")) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length >= 5) {
            String region = fields[2].trim();
            String season = fields[3].trim();
            String yield = fields[4].trim();

            outKey.set(region + "|" + season);
            outValue.set("YIELD|" + yield);
            context.write(outKey, outValue);
        } else if (fields.length == 4) {
            String region = fields[0].trim();
            String season = fields[1].trim();
            String rainfall = fields[2].trim();

            outKey.set(region + "|" + season);
            outValue.set("WEATHER|" + rainfall);
            context.write(outKey, outValue);
        }
    }
}