"""
=============================================================
CMHC Housing Data Governance Project
Script: data_profiler.py
Author: Ram Krishna Dhakal
Purpose: Automated data profiling — replicates Informatica IDMC
         profiling capability in Python. Generates a full HTML
         data quality profile report for the CMHC housing dataset.
=============================================================
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# ── HOW TO RUN ────────────────────────────────────────────────
# python data_profiler.py
# Output: docs/data_profile_report.html + scorecard/column_profile.csv

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
DATASET_PATH = "data/raw/cmhc_housing_starts_2018_2023.csv"
REPORT_PATH  = "docs/data_profile_report.html"

# CDE definitions (from our governance catalog)
CDES = ["REF_DATE", "GEO", "DWELLING_TYPE", "INTENDED_MARKET",
        "HOUSING_STARTS", "AVERAGE_PRICE_CAD"]

# Valid domain values (from our data dictionary)
VALID_GEO_CODES     = {"ON","BC","AB","QC","MB","SK","NS","NB","NL","PE","NT","YT","NU"}
VALID_DWELLING      = {"Single-Detached","Semi-Detached","Row House",
                       "Apartment - 5+ storeys","Apartment - Under 5 storeys"}
VALID_MARKETS       = {"Homeowner","Rental","Condominium"}
VALID_STATUS        = {"","E","F","r"}
VALID_SURVEY_METHOD = {"Direct Survey","Administrative Data","Modelled Estimate"}


# ── STEP 1: LOAD DATA ─────────────────────────────────────────────────────────
def load_data(path):
    print(f"\n[1/6] Loading dataset from: {path}")
    df = pd.read_csv(path)
    print(f"      ✓ Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


# ── STEP 2: COLUMN-LEVEL PROFILE ──────────────────────────────────────────────
def profile_columns(df):
    print(f"\n[2/6] Profiling all {len(df.columns)} columns...")
    total = len(df)
    profiles = []

    for col in df.columns:
        series = df[col]
        null_count  = int(series.isnull().sum())
        null_pct    = round(null_count / total * 100, 2)
        unique_count= int(series.nunique(dropna=True))
        unique_pct  = round(unique_count / total * 100, 2)
        is_cde      = "Yes" if col in CDES else "No"
        dtype       = str(series.dtype)

        profile = {
            "Column"        : col,
            "Is_CDE"        : is_cde,
            "Data_Type"     : dtype,
            "Total_Records" : total,
            "Null_Count"    : null_count,
            "Null_Pct"      : null_pct,
            "Not_Null_Count": total - null_count,
            "Completeness_Pct": round(100 - null_pct, 2),
            "Unique_Values" : unique_count,
            "Unique_Pct"    : unique_pct,
            "Top_Value"     : str(series.mode()[0]) if not series.mode().empty else "N/A",
            "Top_Value_Freq": int(series.value_counts().iloc[0]) if len(series.value_counts()) > 0 else 0,
        }

        # Numeric-specific stats
        if pd.api.types.is_numeric_dtype(series):
            clean = series.dropna()
            profile.update({
                "Min"           : round(float(clean.min()), 2) if len(clean) > 0 else None,
                "Max"           : round(float(clean.max()), 2) if len(clean) > 0 else None,
                "Mean"          : round(float(clean.mean()), 2) if len(clean) > 0 else None,
                "Median"        : round(float(clean.median()), 2) if len(clean) > 0 else None,
                "Std_Dev"       : round(float(clean.std()), 2) if len(clean) > 0 else None,
                "Negative_Count": int((clean < 0).sum()),
                "Zero_Count"    : int((clean == 0).sum()),
                "Positive_Count": int((clean > 0).sum()),
                "Q1"            : round(float(clean.quantile(0.25)), 2) if len(clean) > 0 else None,
                "Q3"            : round(float(clean.quantile(0.75)), 2) if len(clean) > 0 else None,
            })
            # Outlier detection using IQR method
            if len(clean) > 0:
                q1, q3 = clean.quantile(0.25), clean.quantile(0.75)
                iqr = q3 - q1
                outliers = ((clean < q1 - 1.5*iqr) | (clean > q3 + 1.5*iqr)).sum()
                profile["Outlier_Count_IQR"] = int(outliers)
                profile["Outlier_Pct_IQR"]   = round(outliers / total * 100, 2)
        else:
            profile.update({
                "Min": "N/A", "Max": "N/A", "Mean": "N/A", "Median": "N/A",
                "Std_Dev": "N/A", "Negative_Count": 0, "Zero_Count": 0,
                "Positive_Count": 0, "Q1": "N/A", "Q3": "N/A",
                "Outlier_Count_IQR": 0, "Outlier_Pct_IQR": 0.0
            })

        profiles.append(profile)
        status = "⚠" if null_pct > 1 else "✓"
        print(f"      {status} {col:<30} | Nulls: {null_pct:>6.2f}% | Unique: {unique_count:>6,}")

    return pd.DataFrame(profiles)


# ── STEP 3: DOMAIN VALIDATION ─────────────────────────────────────────────────
def validate_domains(df):
    print(f"\n[3/6] Running domain validation checks...")
    issues = []

    checks = [
        ("GEO_CODE",       ~df["GEO_CODE"].isin(VALID_GEO_CODES),        "Invalid province/territory code"),
        ("DWELLING_TYPE",  ~df["DWELLING_TYPE"].isin(VALID_DWELLING),     "Invalid dwelling type"),
        ("INTENDED_MARKET",~df["INTENDED_MARKET"].isin(VALID_MARKETS),    "Invalid intended market"),
        ("STATUS",         ~df["STATUS"].fillna("").isin(VALID_STATUS),   "Invalid status code"),
        ("SURVEY_METHOD",  ~df["SURVEY_METHOD"].fillna("").isin(
                            VALID_SURVEY_METHOD | {""}),                  "Invalid survey method"),
        ("HOUSING_STARTS", df["HOUSING_STARTS"].fillna(0) < 0,            "Negative housing starts"),
        ("AVERAGE_PRICE_CAD", df["AVERAGE_PRICE_CAD"].fillna(0) < 0,     "Negative average price"),
        ("AVERAGE_PRICE_CAD", df["AVERAGE_PRICE_CAD"].fillna(0) > 10_000_000, "Price exceeds $10M ceiling"),
    ]

    for col, mask, reason in checks:
        count = int(mask.sum())
        pct   = round(count / len(df) * 100, 2)
        status= "✓ PASS" if count == 0 else "⚠ WARN"
        print(f"      {status} | {col:<25} | {reason:<40} | {count:>5} records ({pct}%)")
        issues.append({
            "Column": col, "Check": reason,
            "Failed_Records": count, "Failed_Pct": pct,
            "Status": "PASS" if count == 0 else "WARN"
        })

    return pd.DataFrame(issues)


# ── STEP 4: DUPLICATE ANALYSIS ────────────────────────────────────────────────
def analyze_duplicates(df):
    print(f"\n[4/6] Analyzing duplicates...")
    total = len(df)

    # Full row duplicates
    full_dups = int(df.duplicated().sum())

    # Grain-level duplicates (business key)
    grain_cols = ["REF_DATE","GEO_CODE","DWELLING_TYPE","INTENDED_MARKET"]
    grain_dups = int(df.duplicated(subset=grain_cols).sum())

    print(f"      ✓ Full row duplicates : {full_dups:,} ({round(full_dups/total*100,2)}%)")
    print(f"      ✓ Grain duplicates    : {grain_dups:,} ({round(grain_dups/total*100,2)}%)")

    return {
        "Full_Row_Duplicates"  : full_dups,
        "Full_Row_Dup_Pct"     : round(full_dups/total*100, 2),
        "Grain_Duplicates"     : grain_dups,
        "Grain_Dup_Pct"        : round(grain_dups/total*100, 2),
        "Grain_Columns"        : ", ".join(grain_cols)
    }


# ── STEP 5: SUMMARY SCORECARD ─────────────────────────────────────────────────
def build_scorecard(df, col_profiles, domain_issues, dup_stats):
    print(f"\n[5/6] Building DQ scorecard...")
    total = len(df)

    # Completeness score
    avg_completeness = round(col_profiles["Completeness_Pct"].mean(), 2)

    # Validity score (domain checks)
    validity_passed = (domain_issues["Status"] == "PASS").sum()
    validity_score  = round(validity_passed / len(domain_issues) * 100, 2)

    # Uniqueness score
    uniqueness_score = 100.0 if dup_stats["Grain_Duplicates"] == 0 else round(
        (1 - dup_stats["Grain_Duplicates"]/total) * 100, 2)

    overall = round((avg_completeness + validity_score + uniqueness_score) / 3, 2)
    grade   = "A" if overall >= 99 else ("B" if overall >= 97 else ("C" if overall >= 95 else "F"))

    scorecard = {
        "Dataset"             : "cmhc_housing_starts_2018_2023",
        "Profile_Date"        : datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Total_Records"       : total,
        "Total_Columns"       : len(df.columns),
        "CDE_Count"           : len(CDES),
        "Completeness_Score"  : avg_completeness,
        "Validity_Score"      : validity_score,
        "Uniqueness_Score"    : uniqueness_score,
        "Overall_DQ_Score"    : overall,
        "Overall_Grade"       : grade,
        "Total_Null_Cells"    : int(df.isnull().sum().sum()),
        "Columns_With_Nulls"  : int((df.isnull().sum() > 0).sum()),
        "Domain_Checks_Run"   : len(domain_issues),
        "Domain_Checks_Failed": int((domain_issues["Status"] == "WARN").sum()),
    }

    print(f"      ✓ Overall DQ Score : {overall}%  |  Grade: {grade}")
    print(f"      ✓ Completeness     : {avg_completeness}%")
    print(f"      ✓ Validity         : {validity_score}%")
    print(f"      ✓ Uniqueness       : {uniqueness_score}%")

    return scorecard


# ── STEP 6: GENERATE HTML REPORT ─────────────────────────────────────────────
def generate_html_report(scorecard, col_profiles, domain_issues, dup_stats):
    print(f"\n[6/6] Generating HTML report...")

    grade_color = {"A":"#27ae60","B":"#2980b9","C":"#f39c12","F":"#e74c3c"}
    gc = grade_color.get(scorecard["Overall_Grade"], "#888")

    def df_to_html(df, highlight_col=None, highlight_val=None, bad_color="#ffeaa7"):
        rows_html = ""
        for _, row in df.iterrows():
            cells = ""
            for col in df.columns:
                val = row[col]
                style = ""
                if highlight_col and col == highlight_col:
                    if highlight_val and str(val) == highlight_val:
                        style = f' style="background:{bad_color};font-weight:bold"'
                    elif isinstance(val, float) and val < 99:
                        style = f' style="background:{bad_color}"'
                cells += f"<td{style}>{val}</td>"
            rows_html += f"<tr>{cells}</tr>"
        headers = "".join(f"<th>{c}</th>" for c in df.columns)
        return f"<table><thead><tr>{headers}</tr></thead><tbody>{rows_html}</tbody></table>"

    # Key metrics for display
    key_cols = ["Column","Is_CDE","Data_Type","Completeness_Pct",
                "Null_Count","Unique_Values","Negative_Count","Outlier_Count_IQR"]
    profile_display = col_profiles[key_cols].copy()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Profile Report — CMHC Housing Starts</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6f9; color: #2c3e50; }}
  .header {{ background: linear-gradient(135deg, #1F4E79, #2E75B6); color: white; padding: 40px; }}
  .header h1 {{ font-size: 28px; margin-bottom: 8px; }}
  .header p {{ opacity: 0.85; font-size: 14px; }}
  .container {{ max-width: 1200px; margin: 30px auto; padding: 0 20px; }}
  .scorecard {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 30px; }}
  .card {{ background: white; border-radius: 8px; padding: 20px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  .card .value {{ font-size: 32px; font-weight: bold; color: #1F4E79; }}
  .card .label {{ font-size: 12px; color: #7f8c8d; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .grade-card .value {{ color: {gc}; font-size: 48px; }}
  .section {{ background: white; border-radius: 8px; padding: 24px; margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  .section h2 {{ font-size: 18px; color: #1F4E79; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #2E75B6; }}
  .section h3 {{ font-size: 14px; color: #555; margin: 16px 0 8px 0; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #1F4E79; color: white; padding: 10px 12px; text-align: left; font-weight: 500; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #ecf0f1; }}
  tr:hover td {{ background: #f8f9fa; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; }}
  .badge-cde {{ background: #d5e8f0; color: #1F4E79; }}
  .badge-pass {{ background: #d5f5e3; color: #1e8449; }}
  .badge-warn {{ background: #fef9e7; color: #b7950b; }}
  .dim-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }}
  .dim-card {{ border-left: 4px solid #2E75B6; padding: 16px; background: #f8f9fa; border-radius: 4px; }}
  .dim-card .dim-score {{ font-size: 28px; font-weight: bold; color: #1F4E79; }}
  .dim-card .dim-name {{ font-size: 13px; color: #7f8c8d; text-transform: uppercase; }}
  .footer {{ text-align: center; padding: 30px; color: #95a5a6; font-size: 12px; }}
  .warn-box {{ background: #fef9e7; border-left: 4px solid #f39c12; padding: 12px 16px; border-radius: 4px; margin-bottom: 16px; font-size: 13px; }}
</style>
</head>
<body>

<div class="header">
  <h1>🏠 Data Profile Report — CMHC Housing Starts 2018–2023</h1>
  <p>Author: Ram Krishna Dhakal &nbsp;|&nbsp; Generated: {scorecard["Profile_Date"]} &nbsp;|&nbsp;
     Dataset: cmhc_housing_starts_2018_2023.csv &nbsp;|&nbsp;
     Part of: Canadian Housing Data Governance & Quality Framework</p>
</div>

<div class="container">

  <!-- SCORECARD CARDS -->
  <div class="scorecard">
    <div class="card grade-card">
      <div class="value">{scorecard["Overall_Grade"]}</div>
      <div class="label">Overall Grade</div>
    </div>
    <div class="card">
      <div class="value">{scorecard["Overall_DQ_Score"]}%</div>
      <div class="label">Overall DQ Score</div>
    </div>
    <div class="card">
      <div class="value">{scorecard["Total_Records"]:,}</div>
      <div class="label">Total Records</div>
    </div>
    <div class="card">
      <div class="value">{scorecard["Total_Columns"]}</div>
      <div class="label">Columns Profiled</div>
    </div>
    <div class="card">
      <div class="value">{scorecard["CDE_Count"]}</div>
      <div class="label">Critical Data Elements</div>
    </div>
    <div class="card">
      <div class="value">{scorecard["Total_Null_Cells"]:,}</div>
      <div class="label">Total Null Cells</div>
    </div>
    <div class="card">
      <div class="value">{scorecard["Domain_Checks_Failed"]}</div>
      <div class="label">Domain Checks Failed</div>
    </div>
    <div class="card">
      <div class="value">{dup_stats["Grain_Duplicates"]}</div>
      <div class="label">Grain Duplicates</div>
    </div>
  </div>

  <!-- DQ DIMENSION SCORES -->
  <div class="section">
    <h2>📊 DQ Scores by Dimension</h2>
    <div class="dim-grid">
      <div class="dim-card">
        <div class="dim-score">{scorecard["Completeness_Score"]}%</div>
        <div class="dim-name">Completeness</div>
        <p style="font-size:12px;color:#555;margin-top:6px">Average non-null rate across all 16 columns</p>
      </div>
      <div class="dim-card">
        <div class="dim-score">{scorecard["Validity_Score"]}%</div>
        <div class="dim-name">Validity</div>
        <p style="font-size:12px;color:#555;margin-top:6px">Domain, format, and range checks passed</p>
      </div>
      <div class="dim-card">
        <div class="dim-score">{scorecard["Uniqueness_Score"]}%</div>
        <div class="dim-name">Uniqueness</div>
        <p style="font-size:12px;color:#555;margin-top:6px">Grain-level duplicate check on business key</p>
      </div>
    </div>
  </div>

  <!-- REMEDIATION ALERT -->
  <div class="section">
    <h2>⚠️ Issues Requiring Steward Remediation</h2>
    <div class="warn-box">
      <strong>HOUSING_STARTS:</strong> {int(col_profiles[col_profiles["Column"]=="HOUSING_STARTS"]["Null_Count"].values[0]):,} null records +
      {int(col_profiles[col_profiles["Column"]=="HOUSING_STARTS"]["Negative_Count"].values[0]):,} negative values detected.
      Root cause: manual data entry errors in source permit system. Recommendation: flag for Data Steward review and escalate to Data Owner per escalation matrix (Severity: High).
    </div>
    <div class="warn-box">
      <strong>AVERAGE_PRICE_CAD:</strong> {int(col_profiles[col_profiles["Column"]=="AVERAGE_PRICE_CAD"]["Null_Count"].values[0]):,} null records +
      {int(col_profiles[col_profiles["Column"]=="AVERAGE_PRICE_CAD"]["Negative_Count"].values[0]):,} negative values detected.
      Root cause: likely sign-flip error during CPI adjustment transformation. Recommendation: reprocess affected records through CMHC Housing Price Survey pipeline.
    </div>
  </div>

  <!-- COLUMN PROFILE TABLE -->
  <div class="section">
    <h2>🔍 Column-Level Profile</h2>
    {df_to_html(profile_display)}
  </div>

  <!-- DOMAIN VALIDATION -->
  <div class="section">
    <h2>✅ Domain Validation Results</h2>
    {df_to_html(domain_issues, highlight_col="Status", highlight_val="WARN")}
  </div>

  <!-- DUPLICATE ANALYSIS -->
  <div class="section">
    <h2>🔁 Duplicate Analysis</h2>
    <table>
      <thead><tr><th>Check Type</th><th>Count</th><th>Percentage</th><th>Status</th></tr></thead>
      <tbody>
        <tr><td>Full Row Duplicates</td><td>{dup_stats["Full_Row_Duplicates"]:,}</td>
            <td>{dup_stats["Full_Row_Dup_Pct"]}%</td>
            <td><span class="badge badge-{'pass' if dup_stats['Full_Row_Duplicates']==0 else 'warn'}">
            {'PASS' if dup_stats['Full_Row_Duplicates']==0 else 'WARN'}</span></td></tr>
        <tr><td>Grain Duplicates ({dup_stats["Grain_Columns"]})</td>
            <td>{dup_stats["Grain_Duplicates"]:,}</td>
            <td>{dup_stats["Grain_Dup_Pct"]}%</td>
            <td><span class="badge badge-{'pass' if dup_stats['Grain_Duplicates']==0 else 'warn'}">
            {'PASS' if dup_stats['Grain_Duplicates']==0 else 'WARN'}</span></td></tr>
      </tbody>
    </table>
  </div>

</div>

<div class="footer">
  Generated by data_profiler.py &nbsp;|&nbsp; Canadian Housing Data Governance & Quality Framework &nbsp;|&nbsp;
  Ram Krishna Dhakal &nbsp;|&nbsp; github.com/rkdhakal/cmhc-housing-data-governance
</div>

</body>
</html>"""

    os.makedirs("docs", exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"      ✓ Report saved to: {REPORT_PATH}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  CMHC Housing Data Profiler")
    print("  Author: Ram Krishna Dhakal")
    print("=" * 60)

    df            = load_data(DATASET_PATH)
    col_profiles  = profile_columns(df)
    domain_issues = validate_domains(df)
    dup_stats     = analyze_duplicates(df)
    scorecard     = build_scorecard(df, col_profiles, domain_issues, dup_stats)

    # Save CSV outputs
    col_profiles.to_csv("scorecard/column_profile.csv", index=False)
    domain_issues.to_csv("scorecard/domain_validation.csv", index=False)
    pd.DataFrame([scorecard]).to_csv("scorecard/profile_scorecard.csv", index=False)

    generate_html_report(scorecard, col_profiles, domain_issues, dup_stats)

    print("\n" + "=" * 60)
    print("  ✅ Profiling complete!")
    print(f"  Overall DQ Score : {scorecard['Overall_DQ_Score']}%  |  Grade: {scorecard['Overall_Grade']}")
    print(f"  HTML Report      : {REPORT_PATH}")
    print(f"  Open report in your browser to view the full profile.")
    print("=" * 60)
