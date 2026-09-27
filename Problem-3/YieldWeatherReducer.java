import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YieldWeatherReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<Double> yields = new ArrayList<>();
        List<Double> rainfalls = new ArrayList<>();

        for (Text value : values) {
            String data = value.toString();

            if (data.startsWith("YIELD|")) {
                yields.add(Double.parseDouble(data.substring(6)));
            } else if (data.startsWith("WEATHER|")) {
                rainfalls.add(Double.parseDouble(data.substring(8)));
            }
        }

        if (!yields.isEmpty() && !rainfalls.isEmpty()) {

            double rainfallSum = 0;

            for (double rainfall : rainfalls) {
                rainfallSum += rainfall;
            }

            double averageRainfall = rainfallSum / rainfalls.size();

            String[] parts = key.toString().split("\\|");
            String region = parts[0];
            String season = parts[1];

            for (double yield : yields) {
                String output = region + "," + season + ","
                        + averageRainfall + "," + yield;

                context.write(new Text(region), new Text(output));
            }
        }
    }
}