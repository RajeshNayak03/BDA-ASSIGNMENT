# 🌾 AgriSense — Big Data Assignment (BDA)

## Problem 2: Highest Average Yield Crop

> **Business Question:** Which crop (`cropCode`) has the **highest average yield** (`yieldTons`) across all farms?

---

## 📁 Project Structure

```
Problem-2/
├── pom.xml                          ← Maven build file (Hadoop 3.3.6, Java 17)
├── agrisense_problem2.md            ← Problem statement & design spec
├── yield.csv                        ← Input: 100 farm yield records
├── weather.csv                      ← Input: 100 weather records (reference)
└── src/main/java/com/agrisense/p2/
    ├── AvgYieldMapper.java          ← MapReduce Mapper
    ├── AvgYieldReducer.java         ← MapReduce Reducer
    ├── AvgYieldDriver.java          ← Hadoop YARN Driver (cluster mode)
    └── LocalRunner.java             ← Local simulation (no cluster needed)
```

---

## 📊 Input Data

### `yield.csv` — 100 records
```
farmId, cropCode, region, season, yieldTons
F001,   CR02,     Telangana, Kharif, 3.72
F002,   CR05,     Telangana, Kharif, 3.40
...
F100,   CR05,     Tamil Nadu, Zaid,  2.94
```
- **8 distinct crop codes:** CR01 – CR08
- **5 regions:** Telangana, Andhra Pradesh, Karnataka, Maharashtra, Tamil Nadu
- **4 seasons:** Kharif, Rabi, Summer, Zaid

### `weather.csv` — 100 records
```
region, season, avgRainfallMm, avgTempC
Telangana, Kharif, 1024.43, 26.8
...
```
> **Note:** Weather data is loaded for reference. Problem 2 only requires `yield.csv`.

---

## 🏗️ Architecture — MapReduce Pipeline

The solution uses a **2-step MapReduce design**:

```
yield.csv
    │
    ▼
┌─────────────────────────────────────┐
│  PHASE 1: MAP                       │
│  Read each CSV row                  │
│  emit(cropCode, yieldTons)          │
└──────────────┬──────────────────────┘
               │  100 key-value pairs
               ▼
┌─────────────────────────────────────┐
│  PHASE 2: REDUCE                    │
│  Group by cropCode                  │
│  Compute: avg = sum / count         │
│  emit(cropCode, avgYield)           │
└──────────────┬──────────────────────┘
               │  8 crop averages
               ▼
┌─────────────────────────────────────┐
│  PHASE 3: DRIVER (post-job scan)    │
│  Read per-crop averages             │
│  Find max → print winner            │
└─────────────────────────────────────┘
```

---

## 🧠 How Each Class Works

### `AvgYieldMapper.java`
- Reads each line of `yield.csv`
- Extracts `cropCode` (field[1]) and `yieldTons` (field[4])
- Emits `(cropCode, yieldTons)` as a key-value pair
- **Edge cases handled:** skips header row, blank lines, wrong column count, non-numeric yield values

### `AvgYieldReducer.java`
- Receives all yield values grouped by `cropCode`
- Computes `average = sum / count`
- Emits `(cropCode, averageYield)`
- **Edge cases handled:** guards against divide-by-zero if count == 0

### `AvgYieldDriver.java` *(Hadoop YARN — cluster mode)*
- Configures and launches the MapReduce Job 1
- After Job 1 finishes, reads `part-r-00000` from HDFS via `FileSystem` API
- Scans all `(cropCode, avg)` pairs to find the max
- Handles ties by listing all tied crops

### `LocalRunner.java` *(Local simulation — no cluster needed)*
- Simulates the exact same Map → Reduce → Driver pipeline in plain Java
- Reads directly from local filesystem
- Prints all phases step-by-step for visibility

---

## ✅ Results — Actual Output

Running on all **100 records** from `yield.csv`:

### Phase 2 Output — Average Yield per Crop

| Crop Code | Total Yield (tons) | Record Count | **Avg Yield (tons)** |
|-----------|--------------------|--------------|----------------------|
| CR01      | 66.22              | 14           | 4.7300               |
| CR02      | 59.41              | 15           | 3.9607               |
| CR03      | 33.12              | 11           | 3.0109               |
| CR04      | 57.43              | 14           | 4.1021               |
| CR05      | 38.69              | 11           | 3.5173               |
| **CR06**  | **52.61**          | **11**       | **4.7827** 🏆         |
| CR07      | 46.81              | 14           | 3.3436               |
| CR08      | 28.18              | 10           | 2.8180               |

### 🏆 Final Answer

```
Highest average yield crop: CR06 (4.7827 tons)
```

**`CR06`** has the highest average yield across all 100 farms at **4.7827 tons/farm**.

---

## 🚀 How to Build & Run

### Prerequisites
- Java 17+ installed
- Maven 3.x (or use the downloaded Maven as shown below)

### Option A — Local Simulation (Recommended for testing)

```bash
# Step 1: Navigate to project folder
cd Problem-2

# Step 2: Build the JAR
mvn clean package

# Step 3: Run with real CSV files
java -cp target/agrisense.jar com.agrisense.p2.LocalRunner yield.csv weather.csv
```

**One-liner (build + run):**
```bash
mvn clean package -q && java -cp target/agrisense.jar com.agrisense.p2.LocalRunner yield.csv weather.csv
```

### Option B — Hadoop YARN (Cluster Mode)

```bash
# Upload input data to HDFS
hadoop fs -mkdir -p /agrisense/input
hadoop fs -put yield.csv /agrisense/input/yield.csv

# Run the MapReduce job
hadoop jar target/agrisense.jar com.agrisense.p2.AvgYieldDriver \
    /agrisense/input/yield.csv /agrisense/output/p2_avg

# View intermediate output (avg per crop)
hadoop fs -cat /agrisense/output/p2_avg/part-r-00000
```

---

## 🖥️ Sample Terminal Output

```
=================================================
  AgriSense Problem 2 — Local MapReduce Simulator
=================================================

[INFO] Loading weather data from: weather.csv
[INFO] Weather records loaded: 100

[INFO] Loading yield data from: yield.csv
[INFO] Yield records loaded: 100

─── PHASE 1: MAP (cropCode → yieldTons) ─────────
  emit  CR02   → 3.72
  emit  CR05   → 3.40
  emit  CR03   → 3.88
  ...
  emit  CR05   → 2.94

─── PHASE 2: REDUCE (cropCode → avgYield) ────────
  CropCode         Sum   Count     Average
  ──────────────────────────────────────────
  CR01           66.22      14      4.7300
  CR02           59.41      15      3.9607
  CR03           33.12      11      3.0109
  CR04           57.43      14      4.1021
  CR05           38.69      11      3.5173
  CR06           52.61      11      4.7827
  CR07           46.81      14      3.3436
  CR08           28.18      10      2.8180

─── PHASE 3: DRIVER — Find Max Average ──────────

=================================================
  Highest average yield crop: CR06 (4.7827 tons)
=================================================
```

---

## ⚠️ Edge Cases Handled

| Edge Case | How It's Handled |
|-----------|-----------------|
| CSV header row | Skipped by checking if first field matches `farmid` |
| Blank lines | Skipped silently |
| Wrong column count | Logged as `[WARN]`, row skipped |
| Non-numeric `yieldTons` | Caught with `try/catch NumberFormatException`, row skipped |
| Empty `cropCode` | Detected and skipped |
| Divide-by-zero (`count == 0`) | Guarded in reducer, crop skipped with warning |
| Ties (equal averages) | All tied crops printed |
| Empty yield file | Detected, error printed, exit code 4 |

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Language  | Java 17 |
| Build Tool | Apache Maven 3.9.6 |
| Framework | Apache Hadoop 3.3.6 (MapReduce) |
| Input Format | CSV (comma-separated) |
| Output Format | Tab-separated `cropCode \t avgYield` |

---

## 👥 Team

**Repository:** [RajeshNayak03/BDA-ASSIGNMENT](https://github.com/RajeshNayak03/BDA-ASSIGNMENT)
