# AgriSense Farms — Problem 2: Highest Average Yield Crop

## Business Question

Which crop (by `cropCode`) has the highest **average** yield (`yieldTons`) across all farms?

**Input:** `yield.csv` — `farmId, cropCode, region, season, yieldTons`
(Only `cropCode` and `yieldTons` are needed for this problem — `region`, `season`, and `farmId` are ignored.)

**Output:** The crop code with the highest average yield, and that average value.

---

## Why This Is Trickier Than It Looks

A plain MapReduce job naturally computes **per-key aggregates** (e.g., average yield *per crop*) — but reducers don't talk to each other, so no single reducer instance can compare "corn's average" against "wheat's average" unless that comparison is forced somewhere. Two logical steps are needed:

1. **Job 1** — compute the average yield *per crop code*.
2. **Step 2** — from that small per-crop result set, pick the max.

Step 2 can be done two ways — pick whichever fits the assignment's constraints:

- **Option A (recommended, simplest):** Job 1's output is tiny (one line per crop code — only a handful of distinct crops). In the Driver class, after Job 1 finishes, read that output directly with the HDFS `FileSystem` API and compute the max in plain Java. No second job needed.
- **Option B (pure MapReduce, if required):** Chain a second MapReduce job where the Mapper emits a **constant key** (e.g., `"ALL"`) for every record, forcing everything into a single reducer, which then scans all `(crop, avg)` pairs and emits the max.

---

## Design — Job 1: Average Yield per Crop

### Mapper

```
map(key, line):
    fields = line.split(",")
    cropCode = fields[1]
    yieldTons = Double.parse(fields[4])
    emit(Text(cropCode), DoubleWritable(yieldTons))
```

### Reducer

```
reduce(cropCode, yieldValues[]):
    sum = 0
    count = 0
    for v in yieldValues:
        sum += v
        count += 1
    avg = sum / count
    emit(Text(cropCode), DoubleWritable(avg))
```

### Driver

```java
Job job = Job.getInstance(conf, "AverageYieldPerCrop");
job.setJarByClass(AvgYieldDriver.class);
job.setMapperClass(AvgYieldMapper.class);
job.setReducerClass(AvgYieldReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));   // /agrisense/input/yield.csv
FileOutputFormat.setOutputPath(job, new Path(args[1]));  // /agrisense/output/p2_avg
job.waitForCompletion(true);
```

### Sample Output of Job 1 (`part-r-00000`)

```
CORN     42.8
WHEAT    38.1
RICE     51.3
SOY      35.0
```

---

## Step 2 — Option A: Picking the Max (Driver-Side)

```java
FileSystem fs = FileSystem.get(conf);
BufferedReader br = new BufferedReader(new InputStreamReader(
        fs.open(new Path("/agrisense/output/p2_avg/part-r-00000"))));

String bestCrop = null;
double bestAvg = Double.MIN_VALUE;
String line;
while ((line = br.readLine()) != null) {
    String[] parts = line.split("\t");
    double avg = Double.parseDouble(parts[1]);
    if (avg > bestAvg) {
        bestAvg = avg;
        bestCrop = parts[0];
    }
}
System.out.println("Highest average yield crop: " + bestCrop + " (" + bestAvg + " tons)");
```

---

## Step 2 — Option B: Single-Reducer Second Job (Pure MapReduce)

### Mapper 2

Reads Job 1's output, emits everything under one key:

```
map(key, line):
    parts = line.split("\t")
    emit(Text("ALL"), Text(parts[0] + "," + parts[1]))   // "CORN,42.8"
```

### Reducer 2

Single reducer instance sees every crop's average, scans for max:

```
reduce("ALL", values[]):
    bestCrop = null; bestAvg = -infinity
    for v in values:
        (crop, avgStr) = v.split(",")
        avg = Double.parse(avgStr)
        if avg > bestAvg: bestAvg = avg; bestCrop = crop
    emit(Text(bestCrop), DoubleWritable(bestAvg))
```

Set `job.setNumReduceTasks(1)` — this is what forces the global comparison.

---

## Edge Cases to Handle

- **Ties** — decide and document: keep first max seen, or emit all tied crops.
- **Malformed rows** — wrap `Double.parseDouble` in try/catch, skip and log bad lines.
- **Empty yield file** — guard against `count == 0` divide-by-zero in the reducer.

---

## Running It on YARN

```bash
hadoop jar agrisense.jar com.agrisense.p2.AvgYieldDriver \
    /agrisense/input/yield.csv /agrisense/output/p2_avg

hadoop fs -cat /agrisense/output/p2_avg/part-r-00000
```

---

## Spec for Antigravity / Code Generation

> Build a Java MapReduce (Hadoop) solution for: "Which crop has the highest average yield across all farms?"
>
> Input HDFS path: `/agrisense/input/yield.csv`, CSV schema `farmId,cropCode,region,season,yieldTons`.
>
> Job 1: Mapper emits `(cropCode, yieldTons)`; Reducer computes average yield per crop, writes `Text,DoubleWritable` pairs to `/agrisense/output/p2_avg`.
>
> After Job 1 completes, read its output via the `FileSystem` API in the Driver and determine the crop with the maximum average, printing `"Highest average yield crop: <crop> (<avg> tons)"` to stdout.
>
> Include: package `com.agrisense.p2`, classes `AvgYieldMapper`, `AvgYieldReducer`, `AvgYieldDriver`; handle malformed lines and ties; include a `pom.xml`/build file for Hadoop 3.x.
