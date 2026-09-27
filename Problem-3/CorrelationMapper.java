import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CorrelationMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split("\\t");

        if (parts.length != 2) {
            return;
        }

        String region = parts[0].trim();
        String[] data = parts[1].split(",");

        if (data.length != 4) {
            return;
        }

        String rainfall = data[2].trim();
        String yield = data[3].trim();

        outKey.set(region);
        outValue.set(rainfall + "," + yield);

        context.write(outKey, outValue);
    }
}
