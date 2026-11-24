# CIC-IDS2017 Exploratory Data Analysis — Technical Documentation

**SmartHotel IDS — AI-Based Network Intrusion Detection System**
Part 2 of the Networks and Communication Protocols project — ENSA Tétouan

*Version 1.0 — 24 November 2026 — Environment: Databricks (PySpark, Delta Lake)*

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Project Context and Objectives](#2-project-context-and-objectives)
3. [Dataset Acquisition and Description](#3-dataset-acquisition-and-description)
4. [Data Ingestion and Validation](#4-data-ingestion-and-validation)
5. [Exploratory Data Analysis — Class Distribution](#5-exploratory-data-analysis--class-distribution)
6. [Visual Analysis](#6-visual-analysis)
7. [Missing and Non-Finite Values](#7-missing-and-non-finite-values)
8. [Duplicate Records Analysis](#8-duplicate-records-analysis)
9. [Label Consistency Analysis](#9-label-consistency-analysis)
10. [Negative-Value Analysis](#10-negative-value-analysis)
11. [Descriptive Statistics by Traffic Class](#11-descriptive-statistics-by-traffic-class)
12. [Correlation Analysis](#12-correlation-analysis)
13. [Implications for Model Development](#13-implications-for-model-development)
14. [Conclusion and Recommendations](#14-conclusion-and-recommendations)
15. [References and Project Artifacts](#15-references-and-project-artifacts)

---

## 1. Executive Summary

This document describes the data engineering and exploratory data analysis (EDA) work completed on the **CIC-IDS2017** dataset as the foundation of the **SmartHotel IDS** project, an AI-based network intrusion detection system designed for a connected hotel infrastructure. The work was carried out end-to-end on the Databricks platform using PySpark for distributed processing and Delta Lake for persistent storage.

The eight CSV files of CIC-IDS2017 (2,830,743 network flow records, 80 columns, approximately 843.66 MiB) were ingested from a Unity Catalog volume, normalized, annotated with their source file, and combined into a single working dataset that was persisted as a Delta table for reliable reuse across notebooks.

Key findings of the exploratory analysis:

* **Severe class imbalance**: BENIGN traffic represents 80.30% of all records; the remaining 19.70% is distributed across 14 attack categories whose sizes range from 231,073 flows (DoS Hulk) down to 11 flows (Heartbleed).
* **Non-finite values**: exactly 2,867 records (0.101% of the dataset) contain non-finite values (`Infinity`/`NaN`), all of them affecting the two derived rate features `Flow Bytes/s` and `Flow Packets/s` simultaneously — i.e., 2,867 affected records, not 5,734 independent defects.
* **Duplicates**: 95,169 duplicate groups contain 403,550 records, of which 308,381 (10.894% of the dataset) are redundant occurrences beyond the first occurrence of each group.
* **Label inconsistencies**: 698 feature vectors carry contradictory labels (always BENIGN on one side and an attack label on the other), covering 7,020 records — an irreducible source of label noise.
* **Physically impossible values**: 115 BENIGN records exhibit negative values in flow-duration and flow-rate features, a data-quality artifact attributable to the upstream feature-extraction tool.
* **Strong feature redundancy**: 4 of the 66 inspected feature pairs have a Pearson correlation of |r| ≥ 0.8, confirming significant multicollinearity among packet-counting features.

These findings directly shape the preprocessing strategy (cleaning, deduplication, handling of non-finite and negative values) and the model evaluation strategy (minority-class recall, macro F1-score, and class-imbalance handling rather than raw accuracy).

---

## 2. Project Context and Objectives

**SmartHotel IDS** is the second part of a Networks and Communication Protocols project at ENSA Tétouan. Its application domain is a **connected hotel network**, in which a large variety of devices (guest terminals, booking systems, IoT amenities, back-office services) share the same infrastructure and are exposed to both generic Internet threats and targeted attacks.

The AI problem addressed is **network intrusion detection**: given a summarized record of a network flow (durations, packet counts, byte volumes, flag statistics, inter-arrival times), decide whether the flow is legitimate or malicious.

**Primary objective** — develop and evaluate machine learning models capable of distinguishing legitimate network traffic from malicious traffic on CIC-IDS2017.

**Planned classification approaches:**

1. **Binary classification** — BENIGN vs. ATTACK (all 14 attack categories merged into a single ATTACK class).
2. **Multiclass classification** — BENIGN plus each individual attack category (15 classes in total).

The final solution is intended to support an intelligent intrusion detection system deployed to protect the connected hotel infrastructure.

---

## 3. Dataset Acquisition and Description

### 3.1 Why CIC-IDS2017

CIC-IDS2017, published by the Canadian Institute for Cybersecurity (CIC), is one of the most widely used public benchmarks for network intrusion detection research. It contains realistic benign background traffic together with a broad spectrum of attacks executed on a controlled testbed over five working days (Monday to Friday). This combination of scale (2.83 million flows), attack diversity (14 attack categories spanning DoS/DDoS, brute force, port scanning, web attacks, botnet, infiltration and heartbleed) and per-flow labels makes it a suitable foundation for training and evaluating IDS models.

### 3.2 CSV version instead of raw PCAP

The original dataset exists as raw packet captures (PCAP) and as a processed CSV version produced by the CICFlowMeter flow-extraction tool. The **CSV version was deliberately selected** because it already contains the extracted per-flow features (duration, packet/byte statistics, flag counts, inter-arrival times) and the ground-truth `Label` column — exactly the representation required for supervised machine learning. Working directly from PCAP would have required reimplementing the flow-extraction stage (sessionization, feature computation, labeling), a costly engineering effort that would reproduce what CICFlowMeter already provides, while also making results harder to compare with the published literature.

### 3.3 Storage layout

The dataset was acquired directly through Databricks cloud compute and stored in a Unity Catalog volume:

```
/Volumes/workspace/default/cic_ids2017/raw/
```

The dataset consists of eight CSV files:

1. `Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv`
2. `Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv`
3. `Friday-WorkingHours-Morning.pcap_ISCX.csv`
4. `Monday-WorkingHours.pcap_ISCX.csv`
5. `Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv`
6. `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv`
7. `Tuesday-WorkingHours.pcap_ISCX.csv`
8. `Wednesday-workingHours.pcap_ISCX.csv`

Each file corresponds to one capture day (or half-day) and carries the traffic profile of that period: Monday contains only benign traffic, Tuesday adds brute-force attacks (FTP/SSH-Patator), Wednesday DoS attacks, Thursday web attacks and infiltration, and Friday DDoS and port-scan attacks.

### 3.4 Dataset validation results

| Metric | Result |
|---|---:|
| Number of CSV files | 8 |
| Total records | 2,830,743 |
| Total columns | 80 |
| Total stored size | ≈ 843.66 MiB |
| Dataset loaded with | PySpark |

The storage size was re-verified against the volume contents: the eight CSV files total 884,645,759 bytes = 843.66 MiB.

---

## 4. Data Ingestion and Validation

### 4.1 Methodology

Ingestion and validation were implemented in PySpark on Databricks serverless compute (`01_Data_Engineering/01_CIC_IDS2017_Ingestion` and `01_Data_Engineering/02_Data_Validation`), and the combined working dataset was rebuilt in the EDA notebooks (`02_Exploratory_Data_Analysis/02_Exploratory_Data_Analysis`). The procedure is as follows:

1. **Discovery** — list all `*.csv` files under the raw volume path.
2. **Reading** — each CSV is read with `header=True` and `inferSchema=False` (all columns deliberately kept as strings at read time, so that malformed numeric tokens such as `Infinity` and `NaN` remain visible for the data-quality analysis instead of being silently coerced).
3. **Schema inspection and column normalization** — column names are stripped of surrounding whitespace and UTF-8 BOM characters (`\ufeff`) that appear in some CIC-IDS2017 headers.
4. **Label normalization** — the `Label` column is trimmed of whitespace so that day-to-day label spelling differences do not create phantom classes.
5. **Source preservation** — a `_source_file` column is appended to every record with `F.lit(filename)`, keeping full provenance of each flow (essential for per-day analyses and for the cross-file duplicate study of Section 8.5).
6. **Combining** — the eight DataFrames are merged with `unionByName(..., allowMissingColumns=True)`, which aligns columns by name and tolerates any minor schema drift between files, producing the unified `cic_df`.
7. **Validation** — record counts, file availability, and the presence of the `Label` and `_source_file` columns are asserted after the union (2,830,743 rows × 80 columns, all 8 files present).

The original 79 per-flow columns plus `Label` are preserved unchanged; `cic_df` has 80 columns including `_source_file`.

### 4.2 Why PySpark

The dataset contains approximately 2.83 million records and 80 columns. Pandas would load the full dataset into a single driver's memory, which is both fragile and slow at this scale, whereas PySpark distributes reading, cleaning, and aggregation across cluster executors, processes malformed values lazily without crashing, and scales transparently if the dataset grows. PySpark was therefore selected for all ingestion and analysis stages, with Pandas used only for small, already-aggregated results (a few rows) and for plotting.

### 4.3 Session-state issue and Delta persistence

During development a recurring issue was encountered: the `cic_df` variable became **undefined** (`NameError`) whenever the user switched notebooks or the Python session was restarted, because Spark notebook variables live only in the (ephemeral) Python REPL of a given notebook session. Rebuilding the full CSV union in every notebook was wasteful and error-prone.

The adopted recommendation was to **persist the combined dataset once in Delta format**:

```
/Volumes/workspace/default/cic_ids2017/processed/cic_ids2017_delta
```

Two practical points from this step are worth documenting:

* The initial Delta write failed with `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`, because CIC-IDS2017 feature names contain spaces and slashes (e.g., `Destination Port`, `Flow Bytes/s`), which Delta forbids in physical column names. The issue was resolved by enabling **Delta column mapping** (`delta.columnMapping.mode = "name"`) on the write, which keeps the original column names unchanged for all downstream code while Delta auto-generates valid physical names internally.
* **The Delta dataset was created successfully and verified**: the location exists and contains the full combined dataset with 2,830,743 rows and the original 80 columns. The raw CSV files were never modified.

With this persisted copy, subsequent notebooks can obtain `cic_df` reliably with `spark.read.format("delta").load(...)` instead of re-executing the ingestion logic.

---

## 5. Exploratory Data Analysis — Class Distribution

### 5.1 The 15 traffic classes

Grouping `cic_df` by the original `Label` column yields 15 distinct classes with the following exact distribution:

| Traffic class | Records | Share of dataset |
|---|---:|---:|
| BENIGN | 2,273,097 | 80.3004% |
| DoS Hulk | 231,073 | 8.1630% |
| PortScan | 158,930 | 5.6144% |
| DDoS | 128,027 | 4.5227% |
| DoS GoldenEye | 10,293 | 0.3636% |
| FTP-Patator | 7,938 | 0.2804% |
| SSH-Patator | 5,897 | 0.2083% |
| DoS slowloris | 5,796 | 0.2048% |
| DoS Slowhttptest | 5,499 | 0.1943% |
| Bot | 1,966 | 0.0695% |
| Web Attack – Brute Force | 1,507 | 0.0532% |
| Web Attack – XSS | 652 | 0.0230% |
| Infiltration | 36 | 0.0013% |
| Web Attack – SQL Injection | 21 | 0.0007% |
| Heartbleed | 11 | 0.0004% |
| **Total** | **2,830,743** | **100%** |

The class counts sum exactly to the total record count, confirming that no record lacks a label.

### 5.2 Interpretation of the class imbalance

* **BENIGN traffic represents approximately 80.3%** of the dataset (2,273,097 flows) and **malicious traffic approximately 19.7%** (557,646 flows across **14 attack categories**).
* Within the malicious share, the three most frequent attacks — **DoS Hulk (41.4% of all attacks), PortScan (28.5%) and DDoS (23.0%)** — together account for roughly 92.9% of malicious traffic. All remaining eleven categories share about 7%.
* **Heartbleed (11 records), Web Attack – SQL Injection (21) and Infiltration (36) are extremely rare**, each representing less than 0.002% of the dataset.

**Consequences for model training and evaluation.** With this imbalance, a trivial classifier that predicts every flow as BENIGN would already achieve ≈ 80.3% accuracy while detecting **no attacks at all**. Accuracy is therefore an inadequate metric for this problem. Evaluation must instead emphasize:

* **Recall (sensitivity) of the minority attack classes**, since in an IDS context a missed attack (false negative) is far more costly than a false alarm;
* **Macro-averaged F1-score**, defined as

$$\text{Macro-}F_1 \;=\; \frac{1}{K}\sum_{k=1}^{K} F_1(k),$$

which weights every class equally regardless of its support, so that rare attacks influence the score as much as DoS Hulk;
* Class-level metrics and confusion matrices for each attack category, in both the binary and the multiclass experiments.

On the training side, the imbalance will require dedicated handling — for example class weighting, resampling (SMOTE/oversampling of rare classes with stratified splits to avoid data leakage), or threshold tuning — so that minority classes are actually learned rather than drowned by the BENIGN majority.

### 5.3 Binary label transformation

For the binary experiment a derived column `binary_label` was created:

* `Label = "BENIGN"` → **BENIGN** (2,273,097 records, 80.30%);
* every other original class → **ATTACK** (557,646 records, 19.70%).

The transformation is implemented as a `when/otherwise` expression on the uppercased, trimmed label, so it is robust to whitespace and case differences in the raw labels. **Binary classification is a derived experiment**: the original 15-label `Label` column is preserved unchanged in `cic_df` and in the persisted Delta dataset, and the multiclass experiment will use it directly.

---

## 6. Visual Analysis

### 6.1 Visualizations produced

The following visualizations were produced during the EDA (notebooks `03_Dataset_Overview` and `02_Exploratory_Data_Analysis`) and executed successfully:

| # | Visualization | Purpose |
|---:|---|---|
| 1 | Horizontal bar chart of all 15 traffic categories (logarithmic x-axis) | Show the full class distribution despite five orders of magnitude between BENIGN and Heartbleed |
| 2 | Donut/pie chart of BENIGN vs. ATTACK | Communicate the 80.30% / 19.70% binary imbalance at a glance |
| 3 | Horizontal bar chart of each attack type's percentage contribution to malicious traffic | Show that DoS Hulk, PortScan and DDoS dominate ≈ 92.9% of attacks |
| 4 | Horizontal bar chart of traffic volume by source CSV file | Show how the 2.83M flows distribute over the eight capture files |
| 5 | Boxplots of 10 numerical features, BENIGN vs. ATTACK (outliers hidden) | Compare feature distributions between traffic classes |
| 6 | Data-quality issues bar chart by feature | Visualize the two features affected by non-finite values |
| 7 | Pearson correlation heatmap of 12 numerical features | Expose redundancy/multicollinearity among features (Section 12) |

*Note on planned-but-absent outputs*: per-feature **histograms** comparing BENIGN and ATTACK were part of the initial visual-analysis plan, but no histogram output is present in the executed notebook results to date; the distribution-comparison role is currently filled by the boxplots. This documentation therefore does not describe histogram observations, in accordance with the rule that only actually produced visualizations are documented.

### 6.2 Scaling strategies for imbalanced data

Two scaling techniques were used to make highly skewed distributions readable:

* **Logarithmic axes** for count-based charts. The class distribution spans from 11 (Heartbleed) to 2,273,097 (BENIGN) — a ratio of more than 200,000:1 — so bar lengths are plotted on a log-scale axis; otherwise every category except BENIGN, DoS Hulk and PortScan would be visually indistinguishable from zero.
* **Signed log1p transformation** for numerical feature distributions in the boxplots:

$$x' \;=\; \operatorname{sign}(x)\cdot\ln\left(1 + |x|\right)$$

This maps values of very different magnitudes onto a compact range while preserving the sign and the order of magnitude of each observation; it is applied per-series only for visualization, without modifying the underlying data.

---

## 7. Missing and Non-Finite Values

### 7.1 Results

The initial data-quality analysis inspected all 80 columns for missing, empty, and non-finite values:

* **No missing or empty values** were detected in the initial data-quality analysis.
* Exactly two features contain **non-finite values**, in identical record counts:
  * `Flow Bytes/s`: 2,867 non-finite observations;
  * `Flow Packets/s`: 2,867 non-finite observations.
* **Overlap analysis** showed that the affected sets are **identical**: `invalid in BOTH = 2,867` and `invalid in EITHER = 2,867`.

Therefore these are **2,867 affected records, not 5,734 separate records** — each affected flow has both rate features invalid simultaneously. This represents 0.101% of the dataset.

The token-level breakdown of the non-finite values:

| Feature | `Infinity` tokens | `NaN` tokens | Total |
|---|---:|---:|---:|
| Flow Bytes/s | 1,509 | 1,358 | 2,867 |
| Flow Packets/s | 2,867 | 0 | 2,867 |

(Only `Flow Bytes/s` contains `NaN`; `Flow Packets/s` non-finite values are exclusively `Infinity`.)

### 7.2 Possible causes — hypotheses, not verified facts

Both features are **derived rates** computed by CICFlowMeter as bytes (or packets) divided by flow duration. A plausible explanation for the `Infinity` values is **division involving zero-duration flows** (a non-zero numerator divided by an exposure time of zero), while `NaN` may arise from indeterminate forms such as 0/0. This explanation is consistent with the token distribution and with the structure of the features, but it **has not been verified against the raw captures** — it is a hypothesis about the upstream feature-extraction stage, not an established cause. No correction was applied at this stage.

### 7.3 Treatment policy

The non-finite values were **identified during exploratory analysis only**: the original raw dataset and the persisted Delta dataset were **not modified**. In the EDA notebooks, affected values were converted to `null` in temporary analysis copies (via a cast to double followed by NaN/Infinity filtering) so that aggregations like means, medians and percentiles were computed over valid observations only. The decision of whether to drop, clip, or impute these 2,867 records belongs to the preprocessing stage (`03_Data_Preprocessing`) and is documented there.

---

## 8. Duplicate Records Analysis

This is a central data-quality finding, so the methodology is stated precisely.

### 8.1 Methodology

Duplicates were detected by **grouping `cic_df` on every original feature column together with the original `Label` column**, i.e. on all 80 columns **excluding only `_source_file`**. Two records are considered duplicates when they are indistinguishable on all flow features and the label — the capture file they came from is deliberately ignored, since a genuinely repeated flow may legitimately appear in two day-files. Records with `occurrences > 1` after this grouping form the duplicate groups.

### 8.2 Global results

| Metric | Result |
|---|---:|
| Original records | 2,830,743 |
| Unique feature-label combinations | 2,522,362 |
| Duplicate groups | 95,169 |
| Records in duplicate groups | 403,550 |
| Redundant occurrences | 308,381 |
| Duplicate percentage | 10.894% |

Consistency checks: unique combinations + redundant occurrences = 2,522,362 + 308,381 = 2,830,743 = original records ✓; records in duplicate groups − duplicate groups = 403,550 − 95,169 = 308,381 = redundant occurrences ✓.

### 8.3 Terminology

The three quantities must not be confused:

* **Duplicate groups** — distinct feature-label vectors that occur more than once (95,169 such vectors exist).
* **Records belonging to duplicate groups** — the total number of rows that participate in those groups (403,550), i.e., every group of size *n* contributes *n* rows, and the first occurrence of each group is included.
* **Redundant occurrences beyond the first** — the number of rows that would be removed if each group were reduced to a single representative (308,381), i.e., $$\sum_{g \in \text{groups}} (n_g - 1).$$

### 8.4 Duplicate structure by class

| Class | Duplicate groups | Records in groups | Redundant occurrences | Cross-file groups |
|---|---:|---:|---:|---:|
| BENIGN | 59,644 | 236,257 | 176,613 | 33,563 |
| PortScan | 33,390 | 101,501 | 68,111 | 0 |
| DoS Hulk | 1,340 | 59,564 | 58,224 | 0 |
| SSH-Patator | 148 | 2,826 | 2,678 | 0 |
| FTP-Patator | 452 | 2,457 | 2,005 | 0 |
| DoS slowloris | 96 | 507 | 411 | 0 |
| DoS Slowhttptest | 52 | 323 | 271 | 0 |
| Web Attack – Brute Force | 25 | 62 | 37 | 0 |
| Bot | 6 | 19 | 13 | 0 |
| DDoS | 9 | 20 | 11 | 0 |
| DoS GoldenEye | 7 | 14 | 7 | 0 |

The per-class figures sum exactly to the global totals (95,169 groups; 403,550 records in groups; 308,381 redundant occurrences). Several classes (Bot, DDoS, DoS GoldenEye) have almost no redundancy, while BENIGN and PortScan dominate in group count and DoS Hulk in group size (an average of ≈ 44 occurrences per group).

### 8.5 Cross-file duplicates

Of the 95,169 duplicate groups, **33,563 span more than one source CSV file** — and all of them are BENIGN, accounting for 106,280 redundant records appearing identically in multiple capture files. Attack duplicates never cross file boundaries. A plausible reading is that identical benign background flows (e.g., idle connections with constant statistics) were captured identically on several days; regardless of the explanation, the practical implication for modeling is clear: **training/test splits must be performed on deduplicated data** (or by duplicate-group-aware splitting), otherwise the same flow appears in both splits and inflates evaluation scores.

### 8.6 Implications

* **Data leakage**: undeduplicated near-identical flows across train/test splits would leak information and overstate model performance.
* **Bias towards duplicated patterns**: BENIGN, PortScan and DoS Hulk duplicate structure means models would otherwise see some flow patterns far more often than their true frequency, biasing decision boundaries toward them.
* **Recommendation**: remove the 308,381 redundant occurrences before model training (keeping one representative per duplicate group), while retaining the raw and Delta datasets untouched as the source of truth.

---

## 9. Label Consistency Analysis

A separate question from duplication: do **identical feature vectors carry different labels**? Grouping `cic_df` on all 79 feature columns (excluding `Label` and `_source_file`) and counting distinct labels per group gives:

| Metric | Result |
|---|---:|
| Feature groups with conflicting labels | 698 |
| Total records in conflict groups | 7,020 |
| Excess occurrences beyond one per group | 6,322 |
| Maximum distinct labels per feature group | 2 |

Label-combination breakdown:

| Label combination | Feature groups | Total records |
|---|---:|---:|
| BENIGN + PortScan | 564 | 1,618 |
| BENIGN + DoS Hulk | 130 | 5,388 |
| BENIGN + DDoS | 3 | 9 |
| BENIGN + DoS slowloris | 1 | 5 |

Every conflict is a **BENIGN vs. ATTACK** contradiction (there are no attack-vs-attack conflicts). Two readings are possible: (a) genuinely identical flows can be either benign or malicious in their context (e.g., a port-scan-like burst of connections), and CICFlowMeter's 79 per-flow features are insufficient to separate them; or (b) mislabeled records in the benchmark itself. In either case, these 7,020 records (0.25% of the dataset) constitute **irreducible label noise** at the feature level — no classifier using these features can be simultaneously correct on both sides of a conflict, which places a soft ceiling on achievable accuracy and argues for the same deduplication-before-splitting policy as in Section 8.6.

---

## 10. Negative-Value Analysis

Beyond non-finite values, the EDA checked for **physically impossible negative values** in the principal flow features. Casting `Flow Duration`, `Flow Bytes/s` and `Flow Packets/s` to double and filtering for values < 0 yields:

| Condition | Records |
|---|---:|
| Negative `Flow Duration` | 115 |
| Negative `Flow Packets/s` | 115 |
| Negative `Flow Bytes/s` | 85 |
| Duration AND Packets/s both negative | 115 |
| Duration AND Bytes/s both negative | 85 |
| All three negative | 85 |
| Any of the three negative | 115 |

All 115 affected records are **BENIGN** (0.004% of the dataset). The most affected sources are `Friday-WorkingHours-Afternoon-PortScan` (36 records), `Wednesday-workingHours` (16–21 depending on the feature), `Tuesday-WorkingHours` (17), `Monday-WorkingHours` (9–15), `Thursday-WorkingHours-Morning-WebAttacks` (10–11) and `Friday-WorkingHours-Morning` (8–9). Extreme examples include `Flow Duration` as low as −13 µs and `Flow Bytes/s` as low as −261,000,000.

A negative duration or rate is physically impossible for a measured flow, so these values are artifacts of the upstream feature-extraction tool (e.g., timestamp precision/ordering issues or signedness handling during rate computation). The exact mechanism has not been verified against the raw captures and is therefore reported as a **possible explanation, not a verified cause**. As with the non-finite values, the raw data was not modified; the records are candidates for cleaning in the preprocessing stage.

---

## 11. Descriptive Statistics by Traffic Class

The EDA computed per-class descriptive statistics (count, mean, standard deviation, min, median, p95, max via `percentile_approx`) for the 13 selected numerical features, aggregated by the binary traffic class. Selected results (values rounded; durations in µs, rates in bytes/s or packets/s):

| Feature | Class | Valid count | Mean | Median | P95 | Max |
|---|---|---:|---:|---:|---:|---:|
| Flow Duration (µs) | BENIGN | 2,273,097 | 11,220,821 | 30,995 | 113,263,800 | 119,999,998 |
| Flow Duration (µs) | ATTACK | 557,646 | 29,316,808 | 315,826 | 99,982,060 | 119,995,180 |
| Flow IAT Mean (µs) | BENIGN | 2,273,097 | 908,848 | 10,574 | 3,626,185 | 120,000,000 |
| Flow IAT Mean (µs) | ATTACK | 557,646 | 2,886,552 | 44,133 | 9,015,502 | 120,000,000 |
| Total Fwd Packets | BENIGN | 2,273,097 | 10.65 | 2 | 21 | 219,759 |
| Total Fwd Packets | ATTACK | 557,646 | 4.09 | 3 | 9 | 5,523 |
| Total Backward Packets | BENIGN | 2,273,097 | 12.14 | 2 | 19 | 291,922 |
| Total Backward Packets | ATTACK | 557,646 | 3.28 | 1 | 7 | 5,525 |
| Flow Bytes/s | BENIGN | 2,271,320 | 1,779,842 | 5,148 | 2,983,607 | 2.07×10⁹ |
| Flow Bytes/s | ATTACK | 556,556 | 315,883 | 144 | 320,000 | 1.98×10⁹ |
| Flow Packets/s | BENIGN | 2,271,320 | 64,779 | 122 | 565,217 | 4,000,000 |
| Flow Packets/s | ATTACK | 556,556 | 95,649 | 25 | 666,667 | 3,000,000 |
| SYN Flag Count | BENIGN | 2,273,097 | 0.055 | 0 | 1 | 1 |
| SYN Flag Count | ATTACK | 557,646 | 0.012 | 0 | 0 | 1 |

Interpretation of the statistically robust signals (medians and percentile comparisons, which are far less sensitive than the means to the enormous outliers present in both classes):

* **Duration and inter-arrival times are markedly longer for attacks** (median duration 315,826 µs vs. 30,995 µs; median IAT ≈ 44 ms vs. ≈ 10.6 ms), consistent with slow-rate attacks (slowloris, Slowhttptest) and scanning traffic holding connections open.
* **Median `Flow Bytes/s` is dramatically lower for attacks** (144 vs. 5,148 bytes/s): many attack flows transfer little data — a port-scan probe or a slow attack exchanges far fewer bytes per second than normal hotel traffic. The means, by contrast, are dominated by extreme outliers (up to ≈ 2×10⁹ bytes/s), which illustrates why median-based comparisons are preferable for this dataset.
* **Attack flows carry fewer forward/backward packets** at the median (3/1 vs. 2/2), with much tighter p95 bounds (9/7 vs. 21/19) — attack traffic is dominated by small, uniform exchanges, whereas benign traffic occasionally includes very large transfers (up to hundreds of thousands of packets).
* **SYN flag presence is slightly more frequent in benign flows** (mean 0.055 vs. 0.012) — a counter-intuitive but mild signal, consistent with connection-heavy normal traffic on the testbed.
* Note the valid-count differences for `Flow Bytes/s`/`Flow Packets/s` (2,271,320 BENIGN and 556,556 ATTACK, vs. 2,273,097/557,646 for the other features): exactly the 2,867 non-finite records of Section 7 (1,777 BENIGN + 1,090 ATTACK) drop out of the rate statistics — an independent cross-check that the non-finite-value accounting is internally consistent.

These per-class differences confirm that the selected flow features carry discriminative information for the BENIGN-vs-ATTACK task, while their heavy-tailed distributions motivate the log-scale visual treatment (Section 6.2) and robust scaling in preprocessing.

---

## 12. Correlation Analysis

A Pearson correlation matrix was computed over a 10% deterministic sample of the dataset for the 12 selected numerical features (`Destination Port` excluded because its numeric value is a categorical port identifier, not a continuous measurement). The matrix was rendered as an annotated heatmap. Of the 66 distinct feature pairs:

* **4 pairs have |r| ≥ 0.8** (strong redundancy);
* the strongest correlations are:

| Feature pair | Pearson r |
|---|---:|
| Total Fwd Packets ~ Total Backward Packets | 0.999 |
| Total Backward Packets ~ Total Length of Bwd Packets | 0.997 |
| Total Fwd Packets ~ Total Length of Bwd Packets | 0.997 |
| Packet Length Mean ~ Packet Length Std | 0.944 |
| Total Fwd Packets ~ Total Length of Fwd Packets | 0.551 |
| Flow Duration ~ Flow IAT Mean | 0.536 |

Interpretation: forward/backward packet counts and backward byte volume are nearly collinear (r ≥ 0.997) — flows that send many packets in one direction almost invariably receive many packets and bytes in the other. This redundancy: (a) adds no new information, (b) can destabilize or degrade linear models (logistic regression, linear SVM) through multicollinearity, and (c) complicates feature-importance interpretation. Tree-based ensembles handle correlated features gracefully but spread importance across them. A feature-selection or dimensionality-reduction step (e.g., variance-inflation analysis, PCA, or dropping one of each collinear pair) should therefore precede model development, with the collinear features retained in a raw-feature reference table for interpretability.

---

## 13. Implications for Model Development

The EDA findings translate directly into preprocessing and modeling requirements for stages `03_Data_Preprocessing` through `05_Model_Evaluation`:

1. **Deduplication before splitting** (Sections 8–9): remove the 308,381 redundant occurrences and resolve the 698 label-conflict groups; perform train/validation/test splits on the deduplicated data (group-aware where necessary) to prevent leakage and bias toward duplicated BENIGN/PortScan/DoS Hulk patterns.
2. **Non-finite and negative-value cleaning** (Sections 7, 10): decide a policy for the 2,867 non-finite and 115 negative-valued records (drop, clip, or impute); both sets are small (< 0.11% combined) so dropping is a defensible baseline, but the choice must be documented and applied identically in training and inference.
3. **Class imbalance handling** (Section 5.2): class weighting, stratified sampling, or oversampling of the ultra-rare classes (Heartbleed 11, SQL Injection 21, Infiltration 36); consider grouping the three Web Attack variants if per-variant learning proves infeasible.
4. **Metric selection**: report macro F1, per-class recall, and per-class precision alongside accuracy; for the IDS use case, prioritize attack recall (false-negative cost) while monitoring false-alarm rate on BENIGN traffic.
5. **Feature engineering informed by redundancy** (Section 12): drop or combine near-collinear features (|r| ≥ 0.99) before linear models; keep the full feature set for tree ensembles; apply robust/log scaling for distance-based models given the heavy-tailed distributions (Section 11).
6. **Provenance and reproducibility** (Section 4): all downstream stages read the persisted Delta dataset rather than re-ingesting the CSVs, and the raw volume remains the immutable source of truth.

---

## 14. Conclusion and Recommendations

The acquisition, ingestion, and exploratory analysis of CIC-IDS2017 on Databricks is complete and internally consistent: 8 CSV files → 2,830,743 records × 80 columns, validated and persisted as a Delta dataset with full column-name fidelity. The EDA established a precise, quantified picture of the dataset's quality:

* a strongly imbalanced label structure (80.3% BENIGN; 14 attack categories, three of which dominate malicious traffic);
* a small, well-localized set of invalid values (2,867 non-finite records concentrated in two derived rate features; 115 negative-valued BENIGN records);
* a substantial duplicate structure (10.894% redundant occurrences, including 33,563 BENIGN groups duplicated across capture files);
* 698 feature vectors with contradictory labels — a documented noise floor for any classifier;
* heavy-tailed feature distributions and four strongly collinear feature pairs.

**Recommendations for the next stages:** (1) implement the preprocessing pipeline (deduplication, invalid-value handling, robust scaling, optional feature selection) as reproducible notebook stages over the Delta dataset; (2) evaluate binary and multiclass baselines with imbalance-aware training and macro-recall-oriented metrics; (3) maintain every cleaning decision in a documented, version-controlled form so the final academic report can trace each data transformation to the evidence in this document.

The dataset is now fully characterized and ready for the preprocessing and model-development phases of the SmartHotel IDS project.

---

## 15. References and Project Artifacts

**Notebooks**

* `01_Data_Engineering/01_CIC_IDS2017_Ingestion` — CSV discovery, reading, normalization, union, and validation.
* `01_Data_Engineering/02_Data_Validation` — schema and record-count validation.
* `02_Exploratory_Data_Analysis/03_Dataset_Overview` — class distributions, visual analysis, data quality, duplicates, label conflicts.
* `02_Exploratory_Data_Analysis/02_Exploratory_Data_Analysis` — descriptive statistics, boxplots, negative-value analysis, correlation analysis.

**Storage**

* Raw dataset (immutable source of truth): `/Volumes/workspace/default/cic_ids2017/raw/`
* Persisted combined dataset (Delta): `/Volumes/workspace/default/cic_ids2017/processed/cic_ids2017_delta`

**Dataset**

* Sharafaldin, I., Lashkari, A. H., Ghorbani, A. A. — *A Detailed Analysis of the CIC-IDS2017 Dataset*, Canadian Institute for Cybersecurity (CIC), University of New Brunswick. CSV release with CICFlowMeter-extracted per-flow features.

**Tooling**

* Platform: Databricks (serverless compute), PySpark, Delta Lake (column mapping `name` mode), matplotlib, seaborn.

---

*This document reflects the state of the project as of 24 November 2026. All counts, percentages, and statistics cited above were taken from executed notebook outputs and re-verified against the persisted data where possible.*