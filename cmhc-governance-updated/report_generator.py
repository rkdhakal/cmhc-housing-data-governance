"""
=============================================================
CMHC Housing Data Governance Project
Script: report_generator.py
Author: Ram Krishna Dhakal
Purpose: Centralized HTML Report Generator
         Generates both the DQ Execution Report and the
         Data Profile Report as standalone HTML files.
         Extracted from dq_engine.py and data_profiler.py
         for cleaner separation of concerns.
=============================================================
"""

import pandas as pd
import os
from datetime import datetime

# ── HOW TO RUN ────────────────────────────────────────────────
# Option 1: Run standalone (reads CSVs from scorecard/ and data/)
#   python report_generator.py
#
# Option 2: Import and call from dq_engine.py or data_profiler.py
#   from report_generator import generate_dq_execution_report
#   from report_generator import generate_profile_report

# ── CONFIGURATION ─────────────────────────────────────────────
DQ_REPORT_PATH      = "docs/dq_execution_report.html"
PROFILE_REPORT_PATH = "docs/data_profile_report.html"


# ══════════════════════════════════════════════════════════════
#  REPORT 1: DQ EXECUTION REPORT
# ══════════════════════════════════════════════════════════════

def generate_dq_execution_report(df_results, scorecard_stats, rca, output_path=DQ_REPORT_PATH):
    """
    Generate the DQ Rules Execution HTML Report.

    Parameters
    ----------
    df_results      : pd.DataFrame — rule-level execution results
    scorecard_stats : dict — keys: overall_score, passing, warning, failing, clean_records, total
    rca             : dict — root cause analysis breakdown dicts
    output_path     : str — where to save the HTML file
    """
    print(f"\n  [Report Generator] Building DQ Execution Report...")

    def rules_to_html(df):
        rows = ""
        for _, r in df.iterrows():
            color = "#d5f5e3" if r["Status"]=="PASS" else ("#fef9e7" if r["Status"]=="WARN" else "#fde8e8")
            badge = f'<span style="background:{color};padding:2px 8px;border-radius:10px;font-size:11px;font-weight:bold">{r["Status"]}</span>'
            rows += f"""<tr>
                <td>{r['Rule_ID']}</td>
                <td>{r['Rule_Name']}</td>
                <td>{r['DQ_Dimension']}</td>
                <td>{r['CDE_Affected']}</td>
                <td>{r['Severity']}</td>
                <td>{r['Pass_Rate_Pct']}%</td>
                <td>{r['Records_Failed']:,}</td>
                <td>{badge}</td>
                <td style="font-size:11px;color:#555">{r['Remediation_Action'][:80]}...</td>
            </tr>"""
        return rows

    # RCA section
    rca_html = ""
    if "negative_starts_by_province" in rca:
        rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in rca["negative_starts_by_province"].items())
        rca_html += f"""
        <h3>Negative HOUSING_STARTS — by Province</h3>
        <table><thead><tr><th>Province</th><th>Count</th></tr></thead><tbody>{rows}</tbody></table>"""

    if "negative_price_by_dwelling" in rca:
        rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in rca["negative_price_by_dwelling"].items())
        rca_html += f"""
        <h3 style="margin-top:16px">Negative AVERAGE_PRICE_CAD — by Dwelling Type</h3>
        <table><thead><tr><th>Dwelling Type</th><th>Count</th></tr></thead><tbody>{rows}</tbody></table>"""

    if "null_starts_by_year" in rca:
        rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in rca["null_starts_by_year"].items())
        rca_html += f"""
        <h3 style="margin-top:16px">NULL HOUSING_STARTS — by Year</h3>
        <table><thead><tr><th>Year</th><th>Count</th></tr></thead><tbody>{rows}</tbody></table>"""

    clean_pct = round(scorecard_stats["clean_records"]/scorecard_stats["total"]*100, 2)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>DQ Execution Report — CMHC Housing Starts</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ font-family:'Segoe UI',Arial,sans-serif; background:#f4f6f9; color:#2c3e50; }}
  .header {{ background:linear-gradient(135deg,#1F4E79,#2E75B6); color:white; padding:40px; }}
  .header h1 {{ font-size:26px; margin-bottom:8px; }}
  .header p {{ opacity:.85; font-size:13px; }}
  .container {{ max-width:1200px; margin:30px auto; padding:0 20px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:16px; margin-bottom:24px; }}
  .card {{ background:white; border-radius:8px; padding:20px; text-align:center; box-shadow:0 2px 8px rgba(0,0,0,.08); }}
  .card .val {{ font-size:30px; font-weight:bold; color:#1F4E79; }}
  .card .lbl {{ font-size:11px; color:#7f8c8d; margin-top:4px; text-transform:uppercase; }}
  .section {{ background:white; border-radius:8px; padding:24px; margin-bottom:24px; box-shadow:0 2px 8px rgba(0,0,0,.08); }}
  .section h2 {{ font-size:17px; color:#1F4E79; margin-bottom:16px; padding-bottom:8px; border-bottom:2px solid #2E75B6; }}
  .section h3 {{ font-size:13px; color:#555; margin:12px 0 8px; }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; }}
  th {{ background:#1F4E79; color:white; padding:9px 12px; text-align:left; }}
  td {{ padding:8px 12px; border-bottom:1px solid #ecf0f1; }}
  tr:hover td {{ background:#f8f9fa; }}
  .footer {{ text-align:center; padding:30px; color:#95a5a6; font-size:12px; }}
  .warn-box {{ background:#fef9e7; border-left:4px solid #f39c12; padding:12px 16px; border-radius:4px; margin-bottom:12px; font-size:13px; }}
  .info-box {{ background:#d5e8f0; border-left:4px solid #2E75B6; padding:12px 16px; border-radius:4px; margin-bottom:12px; font-size:13px; }}
</style>
</head>
<body>
<div class="header">
  <h1>⚙️ DQ Rules Execution Report — CMHC Housing Starts 2018–2023</h1>
  <p>Author: Ram Krishna Dhakal &nbsp;|&nbsp; Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} &nbsp;|&nbsp;
     12 Rules Executed &nbsp;|&nbsp; Canadian Housing Data Governance & Quality Framework</p>
</div>

<div class="container">

  <div class="cards">
    <div class="card"><div class="val">{scorecard_stats['overall_score']}%</div><div class="lbl">Overall DQ Score</div></div>
    <div class="card"><div class="val">{scorecard_stats['passing']}</div><div class="lbl">Rules Passed</div></div>
    <div class="card"><div class="val">{scorecard_stats['warning']}</div><div class="lbl">Rules Warning</div></div>
    <div class="card"><div class="val">{scorecard_stats['failing']}</div><div class="lbl">Rules Failed</div></div>
    <div class="card"><div class="val">{scorecard_stats['clean_records']:,}</div><div class="lbl">Clean Records</div></div>
    <div class="card"><div class="val">{clean_pct}%</div><div class="lbl">Records Clean</div></div>
  </div>

  <div class="section">
    <h2>📋 DQ Rules Execution Results</h2>
    <table>
      <thead><tr>
        <th>Rule ID</th><th>Rule Name</th><th>Dimension</th><th>CDE</th>
        <th>Severity</th><th>Pass Rate</th><th>Failed</th><th>Status</th><th>Remediation</th>
      </tr></thead>
      <tbody>{rules_to_html(df_results)}</tbody>
    </table>
  </div>

  <div class="section">
    <h2>🔬 Root Cause Analysis</h2>
    <div class="warn-box"><strong>DQ-002 Root Cause:</strong> Negative HOUSING_STARTS values traced to manual data entry errors in the source municipal building permit system. Data entry operators incorrectly entered negative values when recording permit cancellations. <strong>Remediation applied:</strong> Absolute value correction + steward notification.</div>
    <div class="warn-box"><strong>DQ-004 Root Cause:</strong> Negative AVERAGE_PRICE_CAD values traced to a sign-flip error in the CPI adjustment transformation step in the CMHC Housing Price Survey pipeline. <strong>Remediation applied:</strong> Absolute value correction + pipeline fix recommended.</div>
    <div class="info-box"><strong>DQ-001 & DQ-003:</strong> NULL values in HOUSING_STARTS and AVERAGE_PRICE_CAD are not auto-filled — these require Data Steward review and possible back-fill from source systems. Records flagged in exception log.</div>
    {rca_html}
  </div>

  <div class="section">
    <h2>📦 Output Files Generated</h2>
    <table>
      <thead><tr><th>File</th><th>Description</th><th>Records</th></tr></thead>
      <tbody>
        <tr><td>data/processed/cmhc_housing_starts_remediated.csv</td><td>Cleaned dataset with DQ flags applied</td><td>{scorecard_stats['total']:,}</td></tr>
        <tr><td>data/processed/dq_exceptions.csv</td><td>All failed records with rule details and remediation guidance</td><td>See file</td></tr>
        <tr><td>scorecard/dq_execution_scorecard.csv</td><td>Rule-level scorecard with pass rates</td><td>12 rules</td></tr>
      </tbody>
    </table>
  </div>

</div>

<div class="footer">
  Generated by report_generator.py &nbsp;|&nbsp; Canadian Housing Data Governance & Quality Framework &nbsp;|&nbsp;
  Ram Krishna Dhakal &nbsp;|&nbsp; github.com/rkdhakal/cmhc-housing-data-governance
</div>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"      ✓ DQ Execution Report saved to: {output_path}")


# ══════════════════════════════════════════════════════════════
#  REPORT 2: DATA PROFILE REPORT
# ══════════════════════════════════════════════════════════════

def generate_profile_report(scorecard, col_profiles, domain_issues, dup_stats, output_path=PROFILE_REPORT_PATH):
    """
    Generate the Data Profile HTML Report.

    Parameters
    ----------
    scorecard      : dict — profile scorecard with scores and grades
    col_profiles   : pd.DataFrame — column-level profiling results
    domain_issues  : pd.DataFrame — domain validation results
    dup_stats      : dict — duplicate analysis results
    output_path    : str — where to save the HTML file
    """
    print(f"\n  [Report Generator] Building Data Profile Report...")

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
        <p style="font-size:12px;color:#555;margin-top:6px">Weighted pass rate across domain, format, and range checks</p>
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
  Generated by report_generator.py &nbsp;|&nbsp; Canadian Housing Data Governance & Quality Framework &nbsp;|&nbsp;
  Ram Krishna Dhakal &nbsp;|&nbsp; github.com/rkdhakal/cmhc-housing-data-governance
</div>

</body>
</html>"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"      ✓ Data Profile Report saved to: {output_path}")


# ══════════════════════════════════════════════════════════════
#  STANDALONE EXECUTION
#  Reads saved CSVs and regenerates both HTML reports
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  CMHC Housing Report Generator")
    print("  Author: Ram Krishna Dhakal")
    print("=" * 60)

    # ── Generate DQ Execution Report ──────────────────────────
    print("\n[1/2] Loading DQ execution data...")

    dq_scorecard_path = "scorecard/dq_execution_scorecard.csv"
    dq_exceptions_path = "data/processed/dq_exceptions.csv"
    remediated_path = "data/processed/cmhc_housing_starts_remediated.csv"

    if os.path.exists(dq_scorecard_path):
        df_results = pd.read_csv(dq_scorecard_path)
        print(f"      ✓ Loaded {len(df_results)} rules from {dq_scorecard_path}")

        # Rebuild scorecard_stats from CSV
        df_clean = pd.read_csv(remediated_path) if os.path.exists(remediated_path) else pd.DataFrame()
        total = len(df_clean) if len(df_clean) > 0 else 10800
        clean_records = (df_clean["_dq_flag"] == "CLEAN").sum() if "_dq_flag" in df_clean.columns else 0

        scorecard_stats = {
            "overall_score": round(df_results["Pass_Rate_Pct"].mean(), 2),
            "passing": int((df_results["Status"] == "PASS").sum()),
            "warning": int((df_results["Status"] == "WARN").sum()),
            "failing": int((df_results["Status"] == "FAIL").sum()),
            "clean_records": int(clean_records),
            "total": total
        }

        # Rebuild RCA from exceptions
        rca = {}
        if os.path.exists(dq_exceptions_path):
            df_exc = pd.read_csv(dq_exceptions_path)

            neg_starts = df_exc[df_exc["_rule_id"] == "DQ-002"]
            if len(neg_starts) > 0:
                rca["negative_starts_by_province"] = neg_starts.groupby("GEO_CODE").size().sort_values(ascending=False).to_dict()

            neg_price = df_exc[df_exc["_rule_id"] == "DQ-004"]
            if len(neg_price) > 0:
                rca["negative_price_by_dwelling"] = neg_price.groupby("DWELLING_TYPE").size().sort_values(ascending=False).to_dict()

            null_starts = df_exc[df_exc["_rule_id"] == "DQ-001"]
            if len(null_starts) > 0:
                null_starts = null_starts.copy()
                null_starts["_year"] = null_starts["REF_DATE"].astype(str).str[:4]
                rca["null_starts_by_year"] = null_starts.groupby("_year").size().sort_values(ascending=False).to_dict()

        generate_dq_execution_report(df_results, scorecard_stats, rca)
    else:
        print(f"      ⚠ {dq_scorecard_path} not found. Run dq_engine.py first.")
        print(f"        Skipping DQ Execution Report.")

    # ── Generate Profile Report ───────────────────────────────
    print("\n[2/2] Loading profiling data...")

    profile_csv   = "scorecard/column_profile.csv"
    domain_csv    = "scorecard/domain_validation.csv"
    scorecard_csv = "scorecard/profile_scorecard.csv"

    if os.path.exists(profile_csv) and os.path.exists(domain_csv) and os.path.exists(scorecard_csv):
        col_profiles  = pd.read_csv(profile_csv)
        domain_issues = pd.read_csv(domain_csv)
        sc_df         = pd.read_csv(scorecard_csv)
        scorecard     = sc_df.iloc[0].to_dict()
        print(f"      ✓ Loaded profiling data from scorecard/ CSVs")

        # Rebuild dup_stats (these are always in the profile scorecard context)
        raw_path = "data/raw/cmhc_housing_starts_2018_2023.csv"
        if os.path.exists(raw_path):
            df_raw = pd.read_csv(raw_path)
            total = len(df_raw)
            grain_cols = ["REF_DATE","GEO_CODE","DWELLING_TYPE","INTENDED_MARKET"]
            full_dups  = int(df_raw.duplicated().sum())
            grain_dups = int(df_raw.duplicated(subset=grain_cols).sum())
            dup_stats = {
                "Full_Row_Duplicates": full_dups,
                "Full_Row_Dup_Pct": round(full_dups/total*100, 2),
                "Grain_Duplicates": grain_dups,
                "Grain_Dup_Pct": round(grain_dups/total*100, 2),
                "Grain_Columns": ", ".join(grain_cols)
            }
        else:
            dup_stats = {
                "Full_Row_Duplicates": 0, "Full_Row_Dup_Pct": 0.0,
                "Grain_Duplicates": 0, "Grain_Dup_Pct": 0.0,
                "Grain_Columns": "REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET"
            }

        generate_profile_report(scorecard, col_profiles, domain_issues, dup_stats)
    else:
        print(f"      ⚠ Profile CSVs not found in scorecard/. Run data_profiler.py first.")
        print(f"        Skipping Data Profile Report.")

    print("\n" + "=" * 60)
    print("  ✅ Report generation complete!")
    print(f"  DQ Execution Report : {DQ_REPORT_PATH}")
    print(f"  Data Profile Report : {PROFILE_REPORT_PATH}")
    print("=" * 60)
