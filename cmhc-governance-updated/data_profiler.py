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
from report_generator import generate_profile_report

# ── HOW TO RUN ────────────────────────────────────────────────
# python data_profiler.py
# Output: docs/data_profile_report.html + scorecard/column_profile.csv

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
DATASET_PATH = "data/raw/cmhc_housing_starts_2018_2023.csv"

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
    print(f"\n[1/5] Loading dataset from: {path}")
    df = pd.read_csv(path)
    print(f"      ✓ Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


# ── STEP 2: COLUMN-LEVEL PROFILE ──────────────────────────────────────────────
def profile_columns(df):
    print(f"\n[2/5] Profiling all {len(df.columns)} columns...")
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
    print(f"\n[3/5] Running domain validation checks...")
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
    print(f"\n[4/5] Analyzing duplicates...")
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
    print(f"\n[5/5] Building DQ scorecard...")
    total = len(df)

    # Completeness score
    avg_completeness = round(col_profiles["Completeness_Pct"].mean(), 2)

    # Validity score (weighted by records — consistent with DQ engine methodology)
    total_checked   = domain_issues["Failed_Records"].count() * total
    total_failed    = domain_issues["Failed_Records"].sum()
    validity_score  = round((1 - total_failed / total_checked) * 100, 2) if total_checked > 0 else 100.0

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

    generate_profile_report(scorecard, col_profiles, domain_issues, dup_stats)

    print("\n" + "=" * 60)
    print("  ✅ Profiling complete!")
    print(f"  Overall DQ Score : {scorecard['Overall_DQ_Score']}%  |  Grade: {scorecard['Overall_Grade']}")
    print(f"  HTML Report      : docs/data_profile_report.html")
    print(f"  Open report in your browser to view the full profile.")
    print("=" * 60)
