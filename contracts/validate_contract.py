"""
Data Contract Validator
=======================
Validates the CMHC Housing Starts dataset against the data contract
defined in contracts/cmhc_housing_starts.yaml.

Checks:
  1. Schema     — required fields present, no unexpected columns
  2. Required   — required fields have no nulls
  3. ValidValues — categorical fields match allowed domain values
  4. Range      — numeric fields within min/max bounds
  5. Format     — REF_DATE follows YYYY-MM pattern
  6. SLA        — actual pass rates meet tier thresholds from scorecard

Usage:
    python contracts/validate_contract.py
"""

import os
import re
import sys
import yaml
import pandas as pd

# Fix Windows console encoding
sys.stdout.reconfigure(encoding="utf-8")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTRACT     = os.path.join(ROOT, "contracts", "cmhc_housing_starts.yaml")
DATA         = os.path.join(ROOT, "data", "raw", "cmhc_housing_starts_2018_2023.csv")
SCORECARD    = os.path.join(ROOT, "dq_rules", "dq_rules_catalog.csv")

# ── Helpers ───────────────────────────────────────────────────────────────────
PASS  = "✅ PASS"
FAIL  = "❌ FAIL"
WARN  = "⚠  WARN"

results = []

def check(label, passed, detail="", warn=False):
    status = PASS if passed else (WARN if warn else FAIL)
    results.append({"check": label, "status": status, "detail": detail})
    print(f"  {status}  {label}")
    if detail:
        print(f"         {detail}")

def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

# ── Load contract and data ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  DATA CONTRACT VALIDATOR")
print("  CMHC Housing Starts — Canada (2018–2023)")
print("=" * 60)

with open(CONTRACT, "r", encoding="utf-8") as f:
    contract = yaml.safe_load(f)

df = pd.read_csv(DATA)
fields = contract["schema"]["fields"]
tiers  = contract["quality"]["tiers"]

print(f"\n  Contract : {CONTRACT}")
print(f"  Dataset  : {DATA}")
print(f"  Records  : {len(df):,}")
print(f"  Columns  : {len(df.columns)}")

# ══════════════════════════════════════════════════════════════════════════════
# 1. SCHEMA — all contract fields present in data
# ══════════════════════════════════════════════════════════════════════════════
section("1 · SCHEMA CHECKS")

contract_cols = [f["name"] for f in fields]
data_cols     = df.columns.tolist()

# Fields in contract but missing from data
missing = [c for c in contract_cols if c not in data_cols]
check(
    "All contract fields present in dataset",
    len(missing) == 0,
    f"Missing: {missing}" if missing else ""
)

# Fields in data but not in contract
extra = [c for c in data_cols if c not in contract_cols]
check(
    "No undocumented columns in dataset",
    len(extra) == 0,
    f"Undocumented: {extra}" if extra else "",
    warn=True   # warn only — undocumented columns are not a hard failure
)

# ══════════════════════════════════════════════════════════════════════════════
# 2. REQUIRED FIELDS — no nulls where required: true
# ══════════════════════════════════════════════════════════════════════════════
section("2 · REQUIRED FIELD CHECKS")

for field in fields:
    name     = field["name"]
    required = field.get("required", False)
    if not required or name not in df.columns:
        continue
    null_count = df[name].isna().sum()
    check(
        f"{name} has no nulls (required: true)",
        null_count == 0,
        f"{null_count} null(s) found" if null_count > 0 else ""
    )

# ══════════════════════════════════════════════════════════════════════════════
# 3. VALID VALUES — categorical fields match allowed domain
# ══════════════════════════════════════════════════════════════════════════════
section("3 · VALID VALUES CHECKS")

for field in fields:
    name        = field["name"]
    valid_vals  = field.get("validValues")
    if not valid_vals or name not in df.columns:
        continue
    actual_vals = set(df[name].dropna().unique())
    invalid     = actual_vals - set(valid_vals)
    check(
        f"{name} values within defined domain",
        len(invalid) == 0,
        f"Invalid values found: {sorted(invalid)}" if invalid else ""
    )

# ══════════════════════════════════════════════════════════════════════════════
# 4. RANGE CHECKS — numeric min / max
# ══════════════════════════════════════════════════════════════════════════════
section("4 · RANGE CHECKS")

for field in fields:
    name    = field["name"]
    f_min   = field.get("minimum")
    f_max   = field.get("maximum")
    if name not in df.columns or (f_min is None and f_max is None):
        continue

    col = df[name].dropna()

    if f_min is not None:
        below = (col < f_min).sum()
        check(
            f"{name} >= {f_min:,}",
            below == 0,
            f"{below} record(s) below minimum" if below > 0 else ""
        )

    if f_max is not None:
        above = (col > f_max).sum()
        check(
            f"{name} <= {f_max:,}",
            above == 0,
            f"{above} record(s) above maximum" if above > 0 else ""
        )

# ══════════════════════════════════════════════════════════════════════════════
# 5. FORMAT CHECKS — REF_DATE YYYY-MM pattern
# ══════════════════════════════════════════════════════════════════════════════
section("5 · FORMAT CHECKS")

for field in fields:
    name   = field["name"]
    fmt    = field.get("format")
    if not fmt or name not in df.columns:
        continue
    if fmt == "YYYY-MM":
        pattern     = r"^\d{4}-\d{2}$"
        invalid_fmt = df[name].dropna().apply(lambda x: not re.match(pattern, str(x))).sum()
        check(
            f"{name} follows YYYY-MM format",
            invalid_fmt == 0,
            f"{invalid_fmt} record(s) with invalid format" if invalid_fmt > 0 else ""
        )
    elif fmt == "YYYY-MM-DD":
        pattern     = r"^\d{4}-\d{2}-\d{2}$"
        invalid_fmt = df[name].dropna().apply(lambda x: not re.match(pattern, str(x))).sum()
        check(
            f"{name} follows YYYY-MM-DD format",
            invalid_fmt == 0,
            f"{invalid_fmt} record(s) with invalid format" if invalid_fmt > 0 else ""
        )

# ══════════════════════════════════════════════════════════════════════════════
# 6. SLA CHECKS — actual pass rates vs tier thresholds
# ══════════════════════════════════════════════════════════════════════════════
section("6 · SLA / TIER THRESHOLD CHECKS")

scorecard = pd.read_csv(SCORECARD)
rules     = contract["quality"]["rules"]

tier1_warn     = tiers["tier1"]["warnThreshold"]
tier1_critical = tiers["tier1"]["criticalThreshold"]
tier2_warn     = tiers["tier2"]["warnThreshold"]
tier2_critical = tiers["tier2"]["criticalThreshold"]

for rule in rules:
    rule_id   = rule["ruleId"]
    tier      = rule["tier"]
    warn_thr  = rule["warnThreshold"]
    crit_thr  = rule["criticalThreshold"]

    row = scorecard[scorecard["Rule_ID"] == rule_id]
    if row.empty:
        check(f"{rule_id} — result found in scorecard", False, "Rule not found in dq_rules_catalog.csv")
        continue

    pass_rate = float(row["Pass_Rate_Pct"].values[0])
    field     = rule["field"]

    if pass_rate >= warn_thr:
        check(
            f"{rule_id} ({field}) — Tier {tier} SLA met",
            True,
            f"Pass rate {pass_rate:.2f}% >= {warn_thr}% warn threshold"
        )
    elif pass_rate >= crit_thr:
        check(
            f"{rule_id} ({field}) — Tier {tier} SLA warn",
            False,
            f"Pass rate {pass_rate:.2f}% below {warn_thr}% warn threshold (above {crit_thr}% critical)",
            warn=True
        )
    else:
        check(
            f"{rule_id} ({field}) — Tier {tier} SLA CRITICAL breach",
            False,
            f"Pass rate {pass_rate:.2f}% below {crit_thr}% critical threshold"
        )

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'=' * 60}")
print("  VALIDATION SUMMARY")
print(f"{'=' * 60}")

total    = len(results)
passed   = sum(1 for r in results if r["status"] == PASS)
warned   = sum(1 for r in results if r["status"] == WARN)
failed   = sum(1 for r in results if r["status"] == FAIL)

print(f"  Total checks : {total}")
print(f"  {PASS}       : {passed}")
print(f"  {WARN}       : {warned}")
print(f"  {FAIL}       : {failed}")

if failed > 0:
    print(f"\n  ❌ Contract validation FAILED — {failed} hard failure(s) require remediation")
    sys.exit(1)
elif warned > 0:
    print(f"\n  ⚠  Contract validation PASSED WITH WARNINGS — {warned} item(s) need steward review")
else:
    print(f"\n  ✅ Contract fully satisfied — dataset meets all obligations")
