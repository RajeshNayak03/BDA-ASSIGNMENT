import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegionConsistencyMapper
        extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("farmId")) {
            return;
        }

        String[] fields = line.split(",");

        String region = fields[2];
        String season = fields[3];
        double yield = Double.parseDouble(fields[4]);

        outputKey.set(region);
        outputValue.set(season + "," + yield);

        context.write(outputKey, outputValue);
    }
}
