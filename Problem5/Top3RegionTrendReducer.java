import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top3RegionTrendReducer
        extends Reducer<Text, Text, Text, Text> {

    private static final List<String> SEASON_ORDER =
            Arrays.asList("Kharif", "Rabi", "Summer", "Zaid");

    private static class RegionInfo {

        String region;
        Map<String, Double> seasonTotals;
        double totalYield;

        RegionInfo(
                String region,
                Map<String, Double> seasonTotals,
                double totalYield) {

            this.region = region;
            this.seasonTotals = seasonTotals;
            this.totalYield = totalYield;
        }
    }

    private final List<RegionInfo> regions = new ArrayList<>();

    @Override
    protected void reduce(Text region, Iterable<Text> values,
                           Context context)
            throws IOException, InterruptedException {

        Map<String, Double> seasonTotals = new HashMap<>();
        double totalYield = 0.0;

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

                totalYield += yield;

            } catch (NumberFormatException e) {
                // Ignore invalid values
            }
        }

        if (!seasonTotals.isEmpty()) {
            regions.add(
                    new RegionInfo(
                            region.toString(),
                            seasonTotals,
                            totalYield
                    )
            );
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        regions.sort(
                Comparator
                        .comparingDouble(
                                (RegionInfo r) -> r.totalYield
                        )
                        .reversed()
                        .thenComparing(r -> r.region)
        );

        int limit = Math.min(3, regions.size());

        for (int i = 0; i < limit; i++) {

            RegionInfo region = regions.get(i);

            String totalOutput = String.format(
                    Locale.US,
                    "%s\ttotalYield=%.4f",
                    region.region,
                    region.totalYield
            );

            context.write(
                    new Text("TOP_" + (i + 1)),
                    new Text(totalOutput)
            );

            double previousYield = 0.0;
            boolean firstSeason = true;

            for (String season : SEASON_ORDER) {

                double currentYield =
                        region.seasonTotals.getOrDefault(
                                season,
                                0.0
                        );

                if (firstSeason) {

                    String output = String.format(
                            Locale.US,
                            "%s\tseason=%s\ttotalYield=%.4f\tchange=N/A\ttrend=BASELINE",
                            region.region,
                            season,
                            currentYield
                    );

                    context.write(
                            new Text("TREND"),
                            new Text(output)
                    );

                    firstSeason = false;

                } else {

                    double change = currentYield - previousYield;

                    String trend;

                    if (change > 0) {
                        trend = "INCREASE";
                    } else if (change < 0) {
                        trend = "DECREASE";
                    } else {
                        trend = "NO_CHANGE";
                    }

                    String percentChange;

                    if (previousYield == 0.0) {
                        percentChange = "N/A";
                    } else {
                        percentChange = String.format(
                                Locale.US,
                                "%.2f%%",
                                (change / previousYield) * 100.0
                        );
                    }

                    String output = String.format(
                            Locale.US,
                            "%s\tseason=%s\ttotalYield=%.4f\tchange=%.4f\tpercentChange=%s\ttrend=%s",
                            region.region,
                            season,
                            currentYield,
                            change,
                            percentChange,
                            trend
                    );

                    context.write(
                            new Text("TREND"),
                            new Text(output)
                    );
                }

                previousYield = currentYield;
            }
        }
    }
}
