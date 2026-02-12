# JobHop: Markovian Career Transition & Drift Analysis

A data-driven framework for predicting ESCO sector transitions and identifying career outliers using **Static and Dynamic Markov Models**.

## ## Project Overview
JobHop analyzes professional mobility by modeling how individuals move between ESCO (European Skills, Competences, Qualifications and Occupations) sectors over time. The project focuses on two main objectives:
1. **Predictive Modeling**: Forecasting the next likely sector in a user's career path.
2. **Anomaly Detection**: Identifying "Career Drift" where individuals make highly non-traditional transitions.

## ## Core Methodology

### ### 1. Career Transition Model (Static)
The foundation of the project is a transition probability matrix $P$, where each element $P_{i,j}$ represents the probability of moving from sector $i$ to sector $j$.
* **Smoothing**: Applied Laplace smoothing ($\epsilon = 0.05$) to handle unobserved transitions and ensure model robustness.
* **Transition Patterns**: Analyzes historical sector-to-sector movements to determine the most likely "next step" in a career path.

### ### 2. Dynamic Markov Model (Temporal)
To account for changing market trends, the data is partitioned into **temporal windows**.
* **Time Intervals**: Transitions are grouped into eras (e.g., 10-year windows) to capture shifting labor market dynamics.
* **Market Drift**: Measured using the **Frobenius Norm** between successive transition matrices to quantify the structural evolution of the career landscape.

### ### 3. Career Drift Risk (Outliers)
This module identifies individuals whose career paths deviate significantly from the market norm.
* **Scoring Logic**: Calculated using the **negative log-likelihood** of a user's transitions: 
$$\text{Drift Score} = -\frac{1}{n} \sum_{i=1}^{n} \ln(P(transition_i))$$
* **Interpretation**: Higher scores indicate "surprising" or non-traditional career paths relative to historical averages.

## ## Experimental Results & Evolution

### ### The Impact of ESCO Normalization
A critical turning point in the project was the normalization of job titles. Initially, raw job titles created a "Cold Start" problem characterized by extreme sparsity.

| Metric | Raw Titles (Pre-Normalization) | ESCO Sectors (Post-Normalization) |
| :--- | :--- | :--- |
| **Data Sparsity** | **97.00%** | **Significantly Reduced** |
| **Static Top-1 Accuracy** | **15.45%** | **~52.06%** |
| **Dynamic Top-1 Accuracy** | **14.92%** | **Improved (See Notebook)** |



**The Sparsity Challenge**: At 97% sparsity, the models lacked enough data to find meaningful patterns. Standardizing titles into ESCO sectors consolidated the feature space, allowing the Markov models to transition from learning noise to learning career logic.

## ## Performance Benchmarks
The models are evaluated against a **Persistence Baseline**—the "naive" prediction that a user will stay in their current sector—which currently stands at **52.06%**. 



## ## Visualizations
The repository includes automated plotting for:
* **Transition Heatmaps**: Visualizing the density of moves between specific sectors.
* **Drift Distributions**: Identifying statistical outliers in the workforce.
* **Market Evolution**: Tracking how the "Frobenius Norm" of the market changes over decades.

## ## Usage
1. Ensure your data contains: `person_id`, `esco_sector`, `next_sector`, and `year`.
2. Run the `DynamicMarkovModel` to build transition matrices.
3. Use the `CareerDriftRisk` class to flag high-risk outlier career paths.

---
*Developed for research in workforce mobility and automated career pathing.*
