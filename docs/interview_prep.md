# Interview Preparation — CMHC Data Governance Project

**Author:** Ram Krishna Dhakal  
**Purpose:** Deep-dive answers to likely interview questions about this project

---

## GOVERNANCE CONCEPTS

---

### Why did you choose those 6 CDEs?

A Critical Data Element is a field whose quality directly affects a business decision or regulatory outcome. I chose 6 fields because each one has a specific, traceable downstream impact:

| CDE | Why it's critical |
|-----|-------------------|
| `HOUSING_STARTS` | The primary KPI. Federal GDP reporting, affordable housing funding allocation, and CMHC mortgage insurance thresholds are all derived from this number. A bad value here changes policy budgets. |
| `AVERAGE_PRICE_CAD` | The core affordability metric. CMHC uses this to set mortgage insurance eligibility thresholds. An incorrect price affects who qualifies for federal mortgage backing. |
| `REF_DATE` | Every trend analysis, year-over-year comparison, and seasonally adjusted forecast depends on this field being accurate. A wrong date misaligns an entire time series. |
| `GEO` | Provincial policy reporting. Federal transfers to provinces are partly calculated from housing data segmented by province. A wrong province code misdirects funding. |
| `DWELLING_TYPE` | Housing policy is segmented by type — single-detached vs. apartments vs. row houses drive different subsidy programs. Wrong type = wrong policy targeting. |
| `INTENDED_MARKET` | Rental vs. ownership vs. condominium data feeds different policy levers. The federal rental housing strategy uses this field directly. |

The other 10 columns (STATUS, record IDs, etc.) are operational or administrative — important for processing but not decision-driving. In Collibra, CDEs are the fields you assign stewards to, set DQ thresholds for, and monitor continuously. Everything else is documented but not watched as closely.

---

### What is a RACI matrix and why does governance need one?

RACI stands for **Responsible, Accountable, Consulted, Informed**. It answers the question: for every governance activity, who does what?

| Role | Meaning |
|------|---------|
| **Responsible** | Does the actual work |
| **Accountable** | Owns the outcome — the person who answers if something goes wrong |
| **Consulted** | Provides input before a decision is made |
| **Informed** | Told after a decision is made |

In this project, the RACI covers 4 governance activities: data cataloging, DQ rule definition, issue escalation, and data remediation. For example:

- For **DQ rule definition**: the Data Steward is Responsible (writes the rules), the Data Owner is Accountable (approves them), the DGO is Consulted (ensures alignment with standards), and the CDO is Informed.

**Why governance needs it:** Without a RACI, when a data quality issue is found, nobody knows who fixes it, who approves the fix, or who needs to be notified. You get finger-pointing. At CMHC, when a DQ failure was escalated in Informatica IDMC, the escalation matrix defined exactly which steward received the ticket and which owner had to sign off. The RACI is the people layer that makes the technical rules actionable.

---

### What is the difference between Completeness and Validity?

These are two distinct DQ dimensions that are often confused:

**Completeness** asks: *Is the value present?*
- It does not care what the value is — only whether it exists
- DQ-001: Is HOUSING_STARTS NULL? If yes → completeness failure
- DQ-003: Is AVERAGE_PRICE_CAD NULL on a non-suppressed record? If yes → completeness failure
- Completeness failures usually mean data was never captured or was lost in the pipeline

**Validity** asks: *Is the value in the correct format, range, or domain?*
- The value is present, but it's wrong
- DQ-002: HOUSING_STARTS is present but it's -500 → validity failure (negative is impossible)
- DQ-007: DWELLING_TYPE is present but it says "House" instead of "Single-Detached" → validity failure (not in approved domain)
- DQ-009: REF_DATE is present but it says "2021/03" instead of "2021-03" → validity failure (wrong format)

**The practical difference:** A NULL HOUSING_STARTS (completeness) might mean the permit data wasn't received yet — you escalate to the source system. A negative HOUSING_STARTS (validity) means something went wrong in processing — you investigate the ETL pipeline. Same CDE, different remediation path.

In Informatica IDMC, these are separate rule types with separate monitoring dashboards. You never mix them because the business response is different.

---

### Why does PASS require exactly 100% instead of ≥ 99%?

Because PASS means **no known issues exist** — not "almost no issues."

The threshold logic is:
- **PASS** = 100.00% exactly → zero failures found
- **WARN** = ≥ 95% but < 100% → failures exist but within tolerance
- **FAIL** = < 95% → too many failures, immediate action required

This mirrors how Informatica IDMC and enterprise DQ platforms treat rule status. The distinction matters because:

1. **A data steward needs a clear signal.** If PASS meant "≥ 99%", a rule with 100 failures could still show PASS. That's misleading. PASS should mean the rule found nothing to escalate.

2. **Trend monitoring depends on it.** If a rule goes from PASS (100%) to WARN (99.94%), that's a signal — something changed in the data. If PASS allowed 99%, you'd miss early degradation.

3. **Regulatory defensibility.** In a real audit, "this rule passed" means no violations were found. You cannot say that if violations exist.

DQ-013 has 6 failures = 99.94% → WARN, not PASS. Those 6 records are statistical outliers that need a data steward to review them. They are known issues — calling it PASS would hide them.

---

### Why is Grade A possible with 6 WARN rules?

Because **Grade is based on the overall average score**, not on whether every rule is perfect.

The calculation:
- 9 rules at exactly 100% (PASS)
- 6 WARN rules: 98.12%, 97.16%, 98.76%, 98.92%, 99.94%, 98.92%
- Mean of all 15 = **99.45%**
- Grade threshold: ≥ 99% → **A**

Grade A means the dataset is of very high quality overall. It does not mean zero issues exist — it means the issues that do exist are limited in scope and have a controlled impact on the overall dataset.

Think of it like a school grade: a student can get an A on an exam even if they got a few questions wrong, as long as the total score is high enough. The WARN rules are the questions they got partially right — real issues, but not enough to pull the average below the A threshold.

The important thing is that **the WARN rules are still escalated** — Grade A doesn't mean you ignore DQ-001 or DQ-002. It means the dataset is trustworthy at the aggregate level while specific records still need remediation.

---

## THE RULES

---

### Why does DQ-013 use a z-score instead of a fixed threshold?

DQ-013 checks HOUSING_STARTS for statistical outliers. It uses two conditions:
1. Value > 20,000 (absolute ceiling)
2. Value > province mean + 3 standard deviations (z-score per province)

**The problem with fixed thresholds alone:**

Canada has extreme geographic variation in housing construction. Ontario and Quebec build tens of thousands of units per year. Prince Edward Island builds a few hundred. A fixed threshold of, say, 5,000 would flag normal ON/QC records as outliers while missing a genuinely anomalous PEI record of 800 units.

**Why province-level z-scores solve this:**

By computing mean and standard deviation per province (using `groupby("GEO_CODE")`), the rule adapts to each province's normal range. A value that is 3 standard deviations above the PEI mean is flagged even if it's small in absolute terms. A value that is within 3 standard deviations of the Ontario mean is not flagged even if it's large.

**Why 3 standard deviations (mean + 3σ):**

In a normal distribution, 99.7% of values fall within 3 standard deviations of the mean. Anything beyond that is statistically unusual enough to warrant human review. This is the same approach used in statistical process control in manufacturing and in Informatica IDMC's built-in outlier detection profiles.

DQ-013 found **6 records** — a 99.94% pass rate. Those 6 are genuine statistical outliers that no fixed threshold would reliably have caught across all 10 provinces.

---

### What does DQ-015 check and why does it matter?

DQ-015 is a **cross-field consistency rule**. It validates that the full province name in the `GEO` column and the 2-letter province code in the `GEO_CODE` column always refer to the same province, using an authoritative mapping:

```
"Ontario" → "ON"
"British Columbia" → "BC"
"Alberta" → "AB"
... and so on for all 13 provinces/territories
```

For every record where GEO is a known province name, it checks: does GEO_CODE match the expected code? If GEO = "Ontario" but GEO_CODE = "BC", that's a mismatch — the record has been assigned to the wrong province in one of the two fields.

**Why it matters:**

These two columns come from different parts of the data pipeline. GEO comes from a text description field; GEO_CODE comes from a separate coding system. If a join error or ETL transformation goes wrong, they can diverge. A mismatch means provincial aggregates will be wrong — housing starts attributed to Ontario in one field and BC in another will be counted differently depending on which field a downstream report uses.

**Result in this dataset:** 0 failures — 100% PASS. The data is fully consistent. But having the rule documented and executable means it runs on every future load and will catch any join error the moment it happens.

In Collibra, this type of rule would be called a **referential integrity check** between two attributes of the same asset.

---

### Why is DQ-003 different from DQ-001 — what does STATUS = 'F' mean?

Both rules check for NULL values in price fields, but they have different business meaning:

**DQ-001 (HOUSING_STARTS completeness):**
- Simply checks: is HOUSING_STARTS NULL?
- Any NULL is a failure — there is no legitimate reason for housing starts count to be missing
- 203 failures

**DQ-003 (AVERAGE_PRICE_CAD completeness):**
- Checks: is AVERAGE_PRICE_CAD NULL **AND** STATUS ≠ 'F'?
- STATUS = 'F' means the record has been **suppressed** — CMHC deliberately withheld the price to protect confidentiality (this happens in small markets where publishing a price would identify a specific transaction)
- A NULL price on a suppressed record is **expected and correct behavior**
- A NULL price on a non-suppressed record is a genuine data quality failure

**The rule in code:**
```python
df["AVERAGE_PRICE_CAD"].isnull() & (df["STATUS"].fillna("") != "F")
```

This is why governance domain knowledge matters. A naive rule that flags all NULL prices would generate false positives on suppressed records — valid NULLs that represent a deliberate business decision. Understanding STATUS = 'F' is the kind of thing you learn from reading the CMHC data dictionary and talking to data stewards, not from looking at the data alone.

---

### What is grain uniqueness (DQ-010)?

**Grain** is the most granular level at which a dataset is defined — the combination of fields that together uniquely identify one measurement.

For this dataset, the grain is:
```
REF_DATE + GEO_CODE + DWELLING_TYPE + INTENDED_MARKET
```

This means: for a given month, province, dwelling type, and market type — there should be exactly one record reporting housing starts. That is the finest level of detail this dataset claims to represent.

**DQ-010 checks:** are there any records where this 4-field combination appears more than once?

If a record is duplicated at the grain level, it means the same measurement has been loaded twice. When you aggregate housing starts by province and month, you would double-count that combination. The GDP report gets inflated. The policy metric is wrong.

**This is different from exact row duplication.** Two rows could have the same grain but different HOUSING_STARTS values — that is a grain uniqueness failure that an exact-duplicate check (looking at all columns) would miss. DQ-010 catches it; a simple `.duplicated()` on all columns would not.

**Result in this dataset:** 0 failures — 100% PASS. Every grain combination is unique. This rule would catch any ETL pipeline that accidentally runs twice and loads the same period twice.

---

## THE TOOLS

---

### How does this compare to what you did in Informatica IDMC?

The patterns are directly equivalent, implemented with different technology:

| IDMC concept | This project equivalent |
|---|---|
| Rule definition (expression rules, SQL rules) | `dq_engine.py` — 15 rules written as Python/pandas operations with SQL equivalents in `docs/dq_rules_sql.sql` |
| Rule execution against a data source | `run_dq_rules(df)` — runs all 15 rules against the dataset, returns pass/fail per record |
| Exception management / bad records report | `data/processed/dq_exceptions.csv` — every failed record with rule ID, dimension, severity, and failure reason |
| DQ scorecard | `scorecard/dq_execution_scorecard.csv`, `dq_scorecard_summary.csv`, `dq_scorecard_by_dimension.csv` |
| Remediation actions | `remediate_data()` in dq_engine.py — auto-fixes negatives (abs value), flags NULLs for steward review |
| Profiling (column stats, null %) | `scorecard/column_profile.csv`, `domain_validation.csv` |
| Dashboards and monitoring | Streamlit dashboard with 4 tabs |

**Key difference:** In IDMC, rules are configured through a UI and run against live database connections (Databricks SQL, Snowflake, etc.) on a schedule. In this project, rules are code that runs against a CSV. The logic is identical — what changes is the execution environment and the integration layer.

**What I learned from IDMC that I brought here:**
- Severity levels (Critical / High / Medium / Low) — same classification
- Separating detection (run_dq_rules) from remediation (remediate_data) — IDMC does the same
- Rule dimensions aligned to the same 5 ISO 8000 dimensions IDMC uses
- Exception records logged with enough metadata for root cause analysis — same as IDMC's bad records output

---

### What would you change to push scorecard results into Collibra via API?

Collibra exposes a REST API for creating and updating assets, attributes, and DQ metrics. To push this project's scorecard into Collibra:

**Step 1 — Authenticate:**
```python
import requests
session = requests.Session()
session.auth = ("collibra_user", "password")
base_url = "https://your-org.collibra.com/rest/2.0"
```

**Step 2 — Find the asset ID for the dataset in Collibra:**
```python
resp = session.get(f"{base_url}/assets", params={"name": "cmhc_housing_starts_2018_2023"})
asset_id = resp.json()["results"][0]["id"]
```

**Step 3 — Create or update a DQ metric attribute on the asset:**
```python
session.post(f"{base_url}/attributes", json={
    "assetId": asset_id,
    "typeId": "<dq_score_attribute_type_id>",
    "value": "99.45"
})
```

**Step 4 — Do the same for each rule's pass rate, the grade, and dimension scores.**

In a real implementation you would also:
- Register each DQ rule as a Data Quality Rule asset in Collibra linked to its CDE
- Push exception counts as metrics on the CDE assets
- Trigger a Collibra workflow when a rule moves from PASS to WARN

At CMHC, Collibra received DQ results from IDMC via a scheduled integration job. This project would plug into the same pattern — `dq_engine.py` runs, scorecard CSVs are generated, a post-processing step calls the Collibra API to update metrics.

---

### How would you schedule this in Airflow?

Apache Airflow schedules workflows as DAGs (Directed Acyclic Graphs). This project maps cleanly to a DAG with 4 tasks:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    "cmhc_dq_pipeline",
    schedule_interval="0 6 1 * *",  # 6am on the 1st of every month
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    load        = PythonOperator(task_id="load_data",        python_callable=load_data)
    run_rules   = PythonOperator(task_id="run_dq_rules",     python_callable=run_dq_rules)
    remediate   = PythonOperator(task_id="remediate",        python_callable=remediate_data)
    save        = PythonOperator(task_id="save_outputs",     python_callable=save_outputs)

    load >> run_rules >> remediate >> save
```

**In a production version I would add:**
- A `check_source` task before `load_data` that verifies the new CMHC file has arrived in the S3 bucket before running
- An `alert_on_fail` task that sends a Slack or Teams notification if any rule drops to FAIL status
- A `push_to_collibra` task after `save_outputs` that calls the Collibra API
- Retries and SLA monitoring on the DQ run task

CMHC publishes housing starts monthly. The DAG would run on the 1st of each month, process the new data, and post updated scores to the governance platform automatically — no manual intervention.

---

## THE DATA

---

### What caused the 307 negative HOUSING_STARTS?

Root cause analysis traced DQ-002 failures (HOUSING_STARTS < 0) to **manual data entry errors in source municipal building permit systems**.

The breakdown by province showed QC (37 records), AB (35), and NS (35) as the most affected. The distribution across all 10 provinces rules out a single pipeline bug — if it were a system error it would likely affect one province or one time period more heavily. The spread across provinces and time periods is consistent with individual data entry operators making sign errors (entering -500 instead of 500).

**Remediation applied:** `df_clean.loc[neg_mask, "HOUSING_STARTS"] = df_clean.loc[neg_mask, "HOUSING_STARTS"].abs()`

Taking the absolute value is defensible because housing starts cannot be negative — it is physically impossible to un-start construction. The magnitude is almost certainly correct; only the sign is wrong. Records are flagged in `_dq_flag` for steward review even after auto-correction, so the fix is not silent.

**Escalation path:** These records would be escalated to the provincial Data Steward and then to the source permit office to confirm the correct values. The remediation is a best-effort correction, not a final answer.

---

### What is the 5-layer lineage and what happens at each hop?

The lineage traces data from origin to consumption through 5 layers:

**Layer 1 — Source**
Three source systems feed CMHC:
- Municipal building permit offices (housing starts counts — issued permits)
- CMHC Field Surveyor Network (construction stage tracking — when shovels hit ground)
- CMHC Housing Price Survey (average price data — separate from starts)

**Layer 2 — Ingestion**
All three sources are consolidated into the **CMHC Housing Market Information Portal (HMIP)**. This is the internal staging system. At this hop: format standardization, initial record assembly, province code assignment. This is where GEO and GEO_CODE first appear together — which is why DQ-015 (consistency check) is important here.

**Layer 3 — Processing**
Three processing systems run in parallel:
- **Informatica IDMC** — enterprise DQ validation and business rule execution
- **Collibra** — metadata cataloging, stewardship assignment, business glossary management
- **Python DQ Engine (dq_engine.py + app.py)** — this project's implementation

At this hop: DQ rules execute, failures are flagged, approved metadata is attached, remediated data is produced. This is where the 884 exceptions are identified.

**Layer 4 — Publication**
DQ-validated data is published to **Statistics Canada CODR** (Common Output Data Repository). At this hop: final formatting to Statistics Canada's open data standard, suppression of confidential records (STATUS = 'F'), version control.

**Layer 5 — Consumption**
Four consumer types:
- Power BI dashboards (internal CMHC reporting)
- Federal policy reports (GDP, housing strategy documents)
- Data science teams (forecasting, mortgage risk models)
- Public open data (anyone can download from Statistics Canada)

**Why this matters:** If a DQ failure passes through Layer 3 uncorrected, it propagates to all 4 consumer types in Layer 5. A wrong HOUSING_STARTS value in a Power BI dashboard influences the same policy report that a data scientist uses for forecasting. Lineage documentation enables impact assessment — if a rule fails, you can trace exactly which downstream reports are affected.

---

### Why did you choose 2018–2023?

**Practical reasons:**
- Sufficient volume (10,800 records) to make DQ rules statistically meaningful — enough data to compute province-level z-scores, identify outlier patterns, and test uniqueness at the grain level
- 6 years covers multiple housing market cycles — the pre-COVID period (2018–2019), the pandemic shock (2020), the construction surge (2021–2022), and the rate-hike slowdown (2022–2023)
- Enough temporal coverage to make REF_DATE format and range rules relevant

**Governance reasons:**
- CMHC's standard reporting horizon for housing starts trend analysis is 5–10 years — this dataset fits within a typical governance scope
- The 2018 start point aligns with CMHC's National Housing Strategy launch, making the data contextually relevant to the policy environment

**Practical limitation:**
This is a synthetic dataset modelled after the real Statistics Canada data. 2018–2023 was chosen to be recent enough to be credible without requiring access to real restricted data. In a real engagement, the governance framework would apply to live data regardless of the time range.

---

*Use this document to prepare for interviews. Read each answer until you can explain it in your own words without looking at the page.*
