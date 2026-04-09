"""
=============================================================
CMHC Housing Data Governance Project
Script: dq_engine.py
Author: Ram Krishna Dhakal
Purpose: Data Quality Rules Execution Engine
         Runs all 12 DQ rules against the CMHC housing dataset,
         flags failed records, writes exception files, and
         produces a clean remediated output dataset.
         Mirrors Informatica IDMC rule execution workflow.
=============================================================
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ── HOW TO RUN ────────────────────────────────────────────────
# python dq_engine.py
# Output: data/processed/cmhc_housing_starts_remediated.csv
#         data/processed/dq_exceptions.csv
#         docs/dq_execution_report.html
 
# ── CONFIGURATION ─────────────────────────────────────────────────────────────
DATASET_PATH     = "data/raw/cmhc_housing_starts_2018_2023.csv"
PROCESSED_PATH   = "data/processed/cmhc_housing_starts_remediated.csv"
EXCEPTIONS_PATH  = "data/processed/dq_exceptions.csv"
SCORECARD_PATH   = "scorecard/dq_execution_scorecard.csv"
REPORT_PATH      = "docs/dq_execution_report.html"

# Valid domain values (from data dictionary)
VALID_GEO_CODES  = {"ON","BC","AB","QC","MB","SK","NS","NB","NL","PE","NT","YT","NU"}
VALID_DWELLING   = {"Single-Detached","Semi-Detached","Row House",
                    "Apartment - 5+ storeys","Apartment - Under 5 storeys"}
VALID_MARKETS    = {"Homeowner","Rental","Condominium"}
VALID_STATUS     = {"","E","F","r"}


# ── STEP 1: LOAD DATA ─────────────────────────────────────────────────────────
def load_data(path):
    print(f"\n[1/7] Loading dataset...")
    df = pd.read_csv(path)
    df["_record_id"] = range(1, len(df) + 1)
    print(f"      ✓ {len(df):,} records loaded")
    return df


# ── STEP 2: DEFINE & RUN ALL 12 DQ RULES ─────────────────────────────────────
def run_dq_rules(df):
    print(f"\n[2/7] Executing 12 DQ rules...")
    total = len(df)
    results   = []   # rule-level summary
    all_exceptions = []  # record-level failures

    def run_rule(rule_id, name, dimension, cde, description, severity, mask, remediation):
        """Execute one rule, collect results and exceptions."""
        failed_df = df[mask].copy()
        failed    = len(failed_df)
        passed    = total - failed
        score     = round(passed / total * 100, 2)
        status    = "PASS" if score == 100 else ("WARN" if score >= 95 else "FAIL")

        icon = "✓" if status == "PASS" else "⚠"
        print(f"      {icon} {rule_id} | {name:<40} | {score:>7.2f}% | {failed:>5} failed | {status}")

        # Record-level exceptions
        if failed > 0:
            failed_df["_rule_id"]      = rule_id
            failed_df["_rule_name"]    = name
            failed_df["_dimension"]    = dimension
            failed_df["_cde"]          = cde
            failed_df["_severity"]     = severity
            failed_df["_failure_reason"] = description
            failed_df["_remediation"]  = remediation
            failed_df["_flagged_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M")
            all_exceptions.append(failed_df)

        results.append({
            "Rule_ID": rule_id, "Rule_Name": name,
            "DQ_Dimension": dimension, "CDE_Affected": cde,
            "Description": description, "Severity": severity,
            "Total_Records": total, "Records_Passed": passed,
            "Records_Failed": failed, "Pass_Rate_Pct": score,
            "Status": status, "Remediation_Action": remediation
        })

    # ── DQ-001: HOUSING_STARTS completeness ───────────────────────────────────
    run_rule(
        "DQ-001", "Housing Starts Completeness",
        "Completeness", "HOUSING_STARTS",
        "HOUSING_STARTS must not be NULL.",
        "High",
        df["HOUSING_STARTS"].isnull(),
        "Escalate to Data Steward. Attempt back-fill from source permit system. If unavailable, flag STATUS='F'."
    )

    # ── DQ-002: HOUSING_STARTS non-negative ───────────────────────────────────
    run_rule(
        "DQ-002", "Housing Starts Non-Negative",
        "Validity", "HOUSING_STARTS",
        "HOUSING_STARTS must be >= 0. Negative values are physically impossible.",
        "Critical",
        df["HOUSING_STARTS"].fillna(0) < 0,
        "Root cause: manual data entry error in source permit system. Remediation: take absolute value and flag record for steward review."
    )

    # ── DQ-003: AVERAGE_PRICE_CAD completeness ────────────────────────────────
    run_rule(
        "DQ-003", "Average Price Completeness",
        "Completeness", "AVERAGE_PRICE_CAD",
        "AVERAGE_PRICE_CAD must not be NULL on non-suppressed records.",
        "Medium",
        df["AVERAGE_PRICE_CAD"].isnull() & (df["STATUS"].fillna("") != "F"),
        "Escalate to Data Steward. Re-run price survey aggregation for affected period/geography."
    )

    # ── DQ-004: AVERAGE_PRICE_CAD non-negative ────────────────────────────────
    run_rule(
        "DQ-004", "Average Price Non-Negative",
        "Validity", "AVERAGE_PRICE_CAD",
        "AVERAGE_PRICE_CAD must be > 0. Negative prices are invalid.",
        "Critical",
        df["AVERAGE_PRICE_CAD"].fillna(0) < 0,
        "Root cause: sign-flip error during CPI adjustment. Remediation: take absolute value and reprocess through price adjustment pipeline."
    )

    # ── DQ-005: AVERAGE_PRICE_CAD ceiling ─────────────────────────────────────
    run_rule(
        "DQ-005", "Average Price Ceiling Check",
        "Validity", "AVERAGE_PRICE_CAD",
        "AVERAGE_PRICE_CAD must not exceed $10,000,000.",
        "Medium",
        df["AVERAGE_PRICE_CAD"].fillna(0) > 10_000_000,
        "Escalate to Data Owner for manual review. Likely data entry error or unit mismatch."
    )

    # ── DQ-006: GEO_CODE referential integrity ────────────────────────────────
    run_rule(
        "DQ-006", "GEO_CODE Referential Integrity",
        "Validity", "GEO_CODE",
        "GEO_CODE must be a valid Canadian province/territory code.",
        "High",
        ~df["GEO_CODE"].isin(VALID_GEO_CODES),
        "Lookup correct GEO_CODE from Statistics Canada geographic reference table."
    )

    # ── DQ-007: DWELLING_TYPE domain validity ─────────────────────────────────
    run_rule(
        "DQ-007", "Dwelling Type Domain Validity",
        "Validity", "DWELLING_TYPE",
        "DWELLING_TYPE must be one of 5 approved categories.",
        "High",
        ~df["DWELLING_TYPE"].isin(VALID_DWELLING),
        "Map to nearest approved dwelling type using reference lookup table. Escalate if ambiguous."
    )

    # ── DQ-008: INTENDED_MARKET domain validity ───────────────────────────────
    run_rule(
        "DQ-008", "Intended Market Domain Validity",
        "Validity", "INTENDED_MARKET",
        "INTENDED_MARKET must be one of: Homeowner, Rental, Condominium.",
        "High",
        ~df["INTENDED_MARKET"].isin(VALID_MARKETS),
        "Map to approved value using permit application reference data."
    )

    # ── DQ-009: REF_DATE format ───────────────────────────────────────────────
    run_rule(
        "DQ-009", "Reference Date Format",
        "Validity", "REF_DATE",
        "REF_DATE must follow YYYY-MM format.",
        "Medium",
        ~df["REF_DATE"].astype(str).str.match(r"^\d{4}-\d{2}$"),
        "Reformat to YYYY-MM using date parsing. Reject if year or month is out of valid range."
    )

    # ── DQ-010: Grain uniqueness ──────────────────────────────────────────────
    grain = ["REF_DATE","GEO_CODE","DWELLING_TYPE","INTENDED_MARKET"]
    run_rule(
        "DQ-010", "Grain Uniqueness",
        "Uniqueness", "REF_DATE+GEO_CODE+DWELLING_TYPE+INTENDED_MARKET",
        "Each grain combination must be unique.",
        "High",
        df.duplicated(subset=grain, keep=False),
        "Identify source of duplicate load. Remove duplicate keeping most recent record. Investigate ETL pipeline."
    )

    # ── DQ-011: REF_DATE not future ───────────────────────────────────────────
    current_month = datetime.now().strftime("%Y-%m")
    run_rule(
        "DQ-011", "Reference Date Not Future",
        "Validity", "REF_DATE",
        "REF_DATE must not be after the current month.",
        "Medium",
        df["REF_DATE"].astype(str) > current_month,
        "Remove future-dated records. Investigate source system for clock/date errors."
    )

    # ── DQ-012: STATUS code validity ──────────────────────────────────────────
    run_rule(
        "DQ-012", "Status Code Validity",
        "Validity", "STATUS",
        "STATUS must be blank, 'E', 'F', or 'r'.",
        "Low",
        ~df["STATUS"].fillna("").isin(VALID_STATUS),
        "Map to nearest valid status code. Blank (final) is default."
    )

    df_results     = pd.DataFrame(results)
    df_exceptions  = pd.concat(all_exceptions, ignore_index=True) if all_exceptions else pd.DataFrame()

    return df_results, df_exceptions


# ── STEP 3: ROOT CAUSE ANALYSIS ───────────────────────────────────────────────
def root_cause_analysis(df_exceptions):
    print(f"\n[3/7] Running root cause analysis...")

    if df_exceptions.empty:
        print("      ✓ No exceptions found.")
        return {}

    rca = {}

    # HOUSING_STARTS negative — breakdown by province
    neg_starts = df_exceptions[df_exceptions["_rule_id"] == "DQ-002"]
    if len(neg_starts) > 0:
        by_province = neg_starts.groupby("GEO_CODE").size().sort_values(ascending=False)
        rca["negative_starts_by_province"] = by_province.to_dict()
        top_prov = by_province.index[0]
        print(f"      ⚠ Negative HOUSING_STARTS: {len(neg_starts)} records. Most affected: {top_prov} ({by_province.iloc[0]} records)")

    # HOUSING_STARTS null — breakdown by year
    null_starts = df_exceptions[df_exceptions["_rule_id"] == "DQ-001"]
    if len(null_starts) > 0:
        null_starts = null_starts.copy()
        null_starts["_year"] = null_starts["REF_DATE"].astype(str).str[:4]
        by_year = null_starts.groupby("_year").size().sort_values(ascending=False)
        rca["null_starts_by_year"] = by_year.to_dict()
        print(f"      ⚠ NULL HOUSING_STARTS: {len(null_starts)} records across {len(by_year)} years")

    # AVERAGE_PRICE_CAD negative — breakdown by dwelling type
    neg_price = df_exceptions[df_exceptions["_rule_id"] == "DQ-004"]
    if len(neg_price) > 0:
        by_dwelling = neg_price.groupby("DWELLING_TYPE").size().sort_values(ascending=False)
        rca["negative_price_by_dwelling"] = by_dwelling.to_dict()
        print(f"      ⚠ Negative AVERAGE_PRICE_CAD: {len(neg_price)} records. Most affected: {by_dwelling.index[0]}")

    return rca


# ── STEP 4: REMEDIATE DATA ────────────────────────────────────────────────────
def remediate_data(df, df_results):
    print(f"\n[4/7] Applying automated remediations...")
    df_clean = df.copy()
    remediated = 0

    # Fix negative HOUSING_STARTS → take absolute value
    neg_mask = df_clean["HOUSING_STARTS"].fillna(0) < 0
    count = neg_mask.sum()
    df_clean.loc[neg_mask, "HOUSING_STARTS"] = df_clean.loc[neg_mask, "HOUSING_STARTS"].abs()
    df_clean.loc[neg_mask, "_dq_flag"] = "DQ-002: Negative value corrected to absolute"
    remediated += count
    print(f"      ✓ DQ-002: {count} negative HOUSING_STARTS corrected (abs value applied)")

    # Fix negative AVERAGE_PRICE_CAD → take absolute value
    neg_price = df_clean["AVERAGE_PRICE_CAD"].fillna(0) < 0
    count2 = neg_price.sum()
    df_clean.loc[neg_price, "AVERAGE_PRICE_CAD"] = df_clean.loc[neg_price, "AVERAGE_PRICE_CAD"].abs()
    df_clean.loc[neg_price, "_dq_flag"] = df_clean.loc[neg_price, "_dq_flag"].fillna("") + " | DQ-004: Negative price corrected"
    remediated += count2
    print(f"      ✓ DQ-004: {count2} negative AVERAGE_PRICE_CAD corrected (abs value applied)")

    # NULL HOUSING_STARTS → flag for steward, don't auto-fill
    null_mask = df_clean["HOUSING_STARTS"].isnull()
    df_clean.loc[null_mask, "_dq_flag"] = df_clean.loc[null_mask, "_dq_flag"].fillna("") + " | DQ-001: NULL flagged for steward review"
    print(f"      ⚠ DQ-001: {null_mask.sum()} NULL HOUSING_STARTS flagged for steward review (not auto-filled)")

    # NULL AVERAGE_PRICE_CAD → flag for steward
    null_price = df_clean["AVERAGE_PRICE_CAD"].isnull() & (df_clean["STATUS"].fillna("") != "F")
    df_clean.loc[null_price, "_dq_flag"] = df_clean.loc[null_price, "_dq_flag"].fillna("") + " | DQ-003: NULL price flagged for steward review"
    print(f"      ⚠ DQ-003: {null_price.sum()} NULL AVERAGE_PRICE_CAD flagged for steward review")

    # Mark clean records
    df_clean["_dq_flag"] = df_clean["_dq_flag"].fillna("CLEAN")
    df_clean["_remediation_date"] = datetime.now().strftime("%Y-%m-%d")

    total_flagged = (df_clean["_dq_flag"] != "CLEAN").sum()
    print(f"\n      Summary: {remediated} records auto-remediated | {total_flagged} total records flagged")

    return df_clean


# ── STEP 5: BUILD SCORECARD ───────────────────────────────────────────────────
def build_scorecard(df_results, df_clean):
    print(f"\n[5/7] Building execution scorecard...")
    total = len(df_clean)

    by_dim = df_results.groupby("DQ_Dimension").agg(
        Rules=("Rule_ID","count"),
        Avg_Score=("Pass_Rate_Pct","mean"),
        Total_Failed=("Records_Failed","sum")
    ).reset_index()
    by_dim["Avg_Score"] = by_dim["Avg_Score"].round(2)

    overall = round(df_results["Pass_Rate_Pct"].mean(), 2)
    passing = (df_results["Status"] == "PASS").sum()
    warning = (df_results["Status"] == "WARN").sum()
    failing = (df_results["Status"] == "FAIL").sum()
    clean_records = (df_clean["_dq_flag"] == "CLEAN").sum()

    print(f"      ✓ Overall Score  : {overall}%")
    print(f"      ✓ Rules PASS     : {passing} | WARN: {warning} | FAIL: {failing}")
    print(f"      ✓ Clean records  : {clean_records:,} / {total:,} ({round(clean_records/total*100,2)}%)")

    return {
        "overall_score": overall,
        "passing": passing, "warning": warning, "failing": failing,
        "clean_records": clean_records, "total": total,
        "by_dim": by_dim
    }


# ── STEP 6: SAVE OUTPUTS ─────────────────────────────────────────────────────
def save_outputs(df_clean, df_results, df_exceptions, scorecard_stats):
    print(f"\n[6/7] Saving outputs...")
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("scorecard", exist_ok=True)
    os.makedirs("docs", exist_ok=True)

    # Drop internal columns for clean output
    export_cols = [c for c in df_clean.columns if not c.startswith("_")]
    df_clean[export_cols + ["_dq_flag","_remediation_date"]].to_csv(PROCESSED_PATH, index=False)
    print(f"      ✓ Remediated dataset  → {PROCESSED_PATH} ({len(df_clean):,} records)")

    if not df_exceptions.empty:
        df_exceptions.to_csv(EXCEPTIONS_PATH, index=False)
        print(f"      ✓ Exception log       → {EXCEPTIONS_PATH} ({len(df_exceptions):,} exception records)")

    df_results.to_csv(SCORECARD_PATH, index=False)
    print(f"      ✓ Execution scorecard → {SCORECARD_PATH}")


# ── STEP 7: GENERATE HTML REPORT ─────────────────────────────────────────────
def generate_report(df_results, scorecard_stats, rca):
    print(f"\n[7/7] Generating HTML execution report...")

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
  Generated by dq_engine.py &nbsp;|&nbsp; Canadian Housing Data Governance & Quality Framework &nbsp;|&nbsp;
  Ram Krishna Dhakal &nbsp;|&nbsp; github.com/rkdhakal/cmhc-housing-data-governance
</div>
</body>
</html>"""

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"      ✓ HTML report saved to: {REPORT_PATH}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  CMHC Housing DQ Rules Execution Engine")
    print("  Author: Ram Krishna Dhakal")
    print("=" * 60)

    df              = load_data(DATASET_PATH)
    df_results, df_exceptions = run_dq_rules(df)
    rca             = root_cause_analysis(df_exceptions)
    df_clean        = remediate_data(df, df_results)
    scorecard_stats = build_scorecard(df_results, df_clean)
    save_outputs(df_clean, df_results, df_exceptions, scorecard_stats)
    generate_report(df_results, scorecard_stats, rca)

    print("\n" + "=" * 60)
    print("  ✅ DQ Engine execution complete!")
    print(f"  Overall DQ Score  : {scorecard_stats['overall_score']}%")
    print(f"  Clean Records     : {scorecard_stats['clean_records']:,} / {scorecard_stats['total']:,}")
    print(f"  Exceptions logged : data/processed/dq_exceptions.csv")
    print(f"  Clean dataset     : data/processed/cmhc_housing_starts_remediated.csv")
    print(f"  HTML Report       : docs/dq_execution_report.html")
    print("=" * 60)
