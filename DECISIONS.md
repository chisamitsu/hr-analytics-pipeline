# Design Decisions & Roadmap

Running log of analytical and technical decisions made during the project,
and the reasoning behind them. Updated as the project evolves.

---

## Project status

- [x] uv environment configured with all dependencies
- [x] Synthetic dataset generated — 11 CSV files in data/clean/ (seed=12345)
- [x] Dirty dataset generated — 11 CSV files in data/dirty/ with 34 injected
      data quality issues (seed=42)
- [x] Data generation scripts committed to GitHub
- [ ] Analysis notebook — in progress
- [ ] Power BI dashboard — not started

---

## Roadmap

1. Finalise and commit the analysis notebook
2. Confirm notebook runs end to end on data/dirty/
3. Validate the export layer (data/export/ Parquet files) is Power BI ready
4. Build the Power BI dashboard connecting to data/export/
5. Update README with findings once analysis is complete

---

## Data decisions

**Why synthetic data?**  
The analysis is modelled on a real talent marketplace pipeline from an enterprise
HR Tech environment. Synthetic data allows the full pipeline to be reproduced
publicly without privacy or confidentiality concerns, while keeping the
structure and complexity realistic.

**Why two layers — clean and dirty?**  
The clean layer is the baseline. The dirty layer introduces 34 realistic data
quality issues (encoding errors, duplicates, temporal inconsistencies, etc.)
that mirror what you encounter in real vendor SFTP exports and HR system
integrations. The notebook detects and fixes these from the data itself —
no prior knowledge of the manifest required. This demonstrates defensive
data handling, which is more representative of real-world analyst work than
starting from a perfect dataset.

**Seed values**  
Generator: seed=12345. Injector: seed=42. Both fixed for full reproducibility.

---

## Notebook decisions

**data/dirty/ as default input**  
The notebook reads from data/dirty/ by default. Change RAW_DIR in the Setup
cell to data/clean/ for a clean baseline run.

**SCD2 resolution**  
The job assignment table is a Slowly Changing Dimension Type 2 — one row per
assignment period per employee. Resolved to current state by sorting descending
on start_date and taking the first row per employee_id.

**Imputation strategy**  
Missing termination dates for terminated employees are imputed as
hire_date + median tenure of employees who do have a termination date.
Median used instead of mean — tenure distributions are right-skewed and
the median is more robust to long-tenured outliers.

**Encoding anomalies — flagged, not auto-corrected**  
Names with characters outside standard Latin ranges are flagged with a boolean
column. Auto-correcting names without a reference lookup risks introducing
worse errors than the originals.

**Deduplication strategy**  
Keep the latest record per natural key (sort descending by date, drop_duplicates
keep="first"). Applied to applications (employee+gig) and skills (employee+skill,
keep highest level).

**Logistic regression — statsmodels over sklearn**  
statsmodels sm.Logit is used instead of sklearn LogisticRegression because it
produces p-values, confidence intervals, and odds ratios directly in the output.
sklearn is optimised for prediction accuracy; statsmodels is optimised for
statistical inference, which is what this analysis requires.

**Charts — matplotlib and seaborn only**  
plotly was removed from the notebook after rendering issues in VS Code. All
charts use matplotlib and seaborn only. plotly remains available in the
environment for future use in a separate reporting layer.

---

## Export schema (data/export/)

Star-schema tables for Power BI. Each exported as both .csv and .parquet.
Parquet recommended for Power BI — preserves data types and loads faster.

| Table | Key | Type |
|---|---|---|
| dim_employee | employee_id | Dimension |
| dim_gig | gig_id | Dimension |
| dim_skill | skill_id | Dimension |
| dim_training | training_id | Dimension |
| fact_applications | application_id | Fact |
| fact_promotions | — | Fact |
| fact_mau | event_month | Fact |
| fact_skill_gap | skill_id | Fact |
| scd2_job_history | job_assignment_id | Fact |

**Suggested relationships in Power BI:**
- dim_employee[employee_id] → fact_applications[employee_id]
- dim_employee[employee_id] → fact_promotions[employee_id]
- dim_gig[gig_id] → fact_applications[gig_id]
- dim_skill[skill_id] → fact_skill_gap[skill_id]

---

## Environment decisions

**uv over pip/venv**  
uv is used as the primary package manager — faster installs, built-in Python
version management, and lockfile support. requirements.txt is also maintained
for pip compatibility. See README for both install paths.

**Python 3.12**  
Chosen over 3.13/3.14 for stability and compatibility. 3.12 is the current
stable release with full library support across the entire stack.