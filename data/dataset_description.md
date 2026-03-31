# Dataset Description

## Overview

This project uses a large-scale career/job history dataset based on ESCO-style occupation records.
The dataset is used to analyze **career trajectories**, **sector transitions**, **career drift**, **career archetypes**, and **next-sector prediction**.

The full dataset is **not included in this repository** due to its large size.

---

## Dataset Summary

The original dataset contains:

* **Raw Records:** 1,677,701
* **Columns:** 7
* **Users:** 314,875
* **Unique Job Labels:** 2,959
* **Time Span:** 1955 – 2024

---

## Dataset Schema

| Column Name           | Description                                           |
| --------------------- | ----------------------------------------------------- |
| `person_id`           | Unique identifier for each user                       |
| `matched_label`       | Job title / occupation label                          |
| `matched_description` | Text description of the job                           |
| `matched_code`        | ESCO-linked occupation code                           |
| `start_date`          | Start quarter/year of the role                        |
| `end_date`            | End quarter/year of the role                          |
| `university_studies`  | Boolean flag indicating university-related background |

---

## Example Record

| person_id | matched_label    | matched_description                                                                | matched_code | start_date | end_date | university_studies |
| --------- | ---------------- | ---------------------------------------------------------------------------------- | ------------ | ---------- | -------- | ------------------ |
| 0         | resource manager | Resource managers manage resources for projects and coordinate departmental needs. | 1324.8.3     | Q1 2016    | Q2 2019  | TRUE               |

---

## Preprocessing Overview

Before analysis, the dataset undergoes several cleaning and transformation steps:

### 1. Label Cleaning

* Removed rows where `matched_label = "unknown"`
* Removed blank or invalid labels

### 2. Duplicate Removal

* Removed duplicate job entries per user and year

### 3. Time Extraction

* Extracted year information from `start_date`
* Removed rows with invalid or missing year values

### 4. Sector Classification

A multi-pass regex-based classifier was used to map jobs into **27 ESCO-aligned sectors** using:

* Job title matching
* Description fallback matching
* Broad rescue keyword matching

### 5. Removal of Unclassified Records

Records mapped to **“Other & Unclassified”** were removed from the final analytical dataset.

---

## Cleaning Summary

| Stage                             |   Records |
| --------------------------------- | --------: |
| Raw rows loaded                   | 1,677,701 |
| After cleaning                    | 1,193,225 |
| Final classified rows             | 1,127,573 |
| Removed as “Other & Unclassified” |    65,652 |
| Users with 2+ jobs                |   255,485 |
| Total transitions extracted       |   816,757 |

---

## Final Analytical Use

The cleaned dataset supports the following tasks:

* Sector distribution analysis
* Career sequence length analysis
* Transition probability modeling
* PCA of transition profiles
* Career drift scoring
* K-Means clustering of user behavior
* Association rule mining
* Career prediction evaluation

---

## Sector Mapping Output

After classification, all usable records were mapped into **27 career sectors**, including:

* Operations & General Management
* Retail, Hospitality & Events
* Education - Teaching
* Engineering & Manufacturing
* Supply Chain & Logistics
* Healthcare - Nursing & Allied
* Public Sector & Administration
* Software & IT Development
* Data Science & Analytics
* Cybersecurity & Compliance
* Legal & Compliance
* Research & Academia
  (and others)

---

## Note

The dataset is **not distributed through this repository**.
Only the analysis code, outputs, visualizations, and documentation are included.

---
