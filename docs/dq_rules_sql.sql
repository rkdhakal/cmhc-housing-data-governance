-- ============================================================
-- CMHC HOUSING DATA GOVERNANCE PROJECT
-- Data Quality Rules — SQL Implementation
-- Author: Ram Krishna Dhakal
-- Dataset: cmhc_housing_starts_2018_2023
-- ============================================================

-- ── DQ-001: Housing Starts Completeness ─────────────────────
-- Dimension: Completeness | Severity: High | CDE: HOUSING_STARTS
-- Business Rule: HOUSING_STARTS must not be NULL.
SELECT
    'DQ-001' AS rule_id,
    'Housing Starts Completeness' AS rule_name,
    'Completeness' AS dq_dimension,
    COUNT(*) AS failed_records
FROM housing_starts
WHERE HOUSING_STARTS IS NULL;

-- ── DQ-002: Housing Starts Non-Negative ─────────────────────
-- Dimension: Validity | Severity: Critical | CDE: HOUSING_STARTS
-- Business Rule: HOUSING_STARTS must be >= 0.
-- Negative housing starts are physically impossible.
SELECT
    'DQ-002' AS rule_id,
    'Housing Starts Non-Negative' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET,
    HOUSING_STARTS AS failed_value,
    'HOUSING_STARTS cannot be negative' AS failure_reason
FROM housing_starts
WHERE HOUSING_STARTS < 0;

-- ── DQ-003: Average Price Completeness ──────────────────────
-- Dimension: Completeness | Severity: Medium | CDE: AVERAGE_PRICE_CAD
-- Business Rule: AVERAGE_PRICE_CAD should not be NULL
-- except for suppressed records (STATUS = 'F').
SELECT
    'DQ-003' AS rule_id,
    'Average Price Completeness' AS rule_name,
    'Completeness' AS dq_dimension,
    REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET,
    STATUS,
    'AVERAGE_PRICE_CAD is NULL on non-suppressed record' AS failure_reason
FROM housing_starts
WHERE AVERAGE_PRICE_CAD IS NULL
  AND (STATUS IS NULL OR STATUS != 'F');

-- ── DQ-004: Average Price Non-Negative ──────────────────────
-- Dimension: Validity | Severity: Critical | CDE: AVERAGE_PRICE_CAD
-- Business Rule: AVERAGE_PRICE_CAD must be > 0.
SELECT
    'DQ-004' AS rule_id,
    'Average Price Non-Negative' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET,
    AVERAGE_PRICE_CAD AS failed_value,
    'AVERAGE_PRICE_CAD cannot be negative' AS failure_reason
FROM housing_starts
WHERE AVERAGE_PRICE_CAD < 0;

-- ── DQ-005: Average Price Ceiling Check ─────────────────────
-- Dimension: Validity | Severity: Medium | CDE: AVERAGE_PRICE_CAD
-- Business Rule: AVERAGE_PRICE_CAD must not exceed $10,000,000.
SELECT
    'DQ-005' AS rule_id,
    'Average Price Ceiling' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET,
    AVERAGE_PRICE_CAD AS failed_value,
    'AVERAGE_PRICE_CAD exceeds $10,000,000 ceiling' AS failure_reason
FROM housing_starts
WHERE AVERAGE_PRICE_CAD > 10000000;

-- ── DQ-006: GEO_CODE Referential Integrity ──────────────────
-- Dimension: Validity | Severity: High | CDE: GEO_CODE
-- Business Rule: GEO_CODE must be a valid Canadian province/territory code.
SELECT
    'DQ-006' AS rule_id,
    'GEO_CODE Referential Integrity' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO, GEO_CODE,
    'GEO_CODE is not a valid Canadian province/territory code' AS failure_reason
FROM housing_starts
WHERE GEO_CODE NOT IN (
    'ON','BC','AB','QC','MB','SK','NS','NB','NL','PE','NT','YT','NU'
);

-- ── DQ-007: Dwelling Type Domain Validity ───────────────────
-- Dimension: Validity | Severity: High | CDE: DWELLING_TYPE
-- Business Rule: DWELLING_TYPE must be one of 5 approved categories.
SELECT
    'DQ-007' AS rule_id,
    'Dwelling Type Domain Validity' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO_CODE, DWELLING_TYPE,
    'DWELLING_TYPE is not in the approved domain list' AS failure_reason
FROM housing_starts
WHERE DWELLING_TYPE NOT IN (
    'Single-Detached',
    'Semi-Detached',
    'Row House',
    'Apartment - 5+ storeys',
    'Apartment - Under 5 storeys'
);

-- ── DQ-008: Intended Market Domain Validity ─────────────────
-- Dimension: Validity | Severity: High | CDE: INTENDED_MARKET
-- Business Rule: INTENDED_MARKET must be one of 3 approved values.
SELECT
    'DQ-008' AS rule_id,
    'Intended Market Domain Validity' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO_CODE, INTENDED_MARKET,
    'INTENDED_MARKET is not in approved domain list' AS failure_reason
FROM housing_starts
WHERE INTENDED_MARKET NOT IN ('Homeowner', 'Rental', 'Condominium');

-- ── DQ-009: Reference Date Format Validity ──────────────────
-- Dimension: Validity | Severity: Medium | CDE: REF_DATE
-- Business Rule: REF_DATE must follow YYYY-MM format.
SELECT
    'DQ-009' AS rule_id,
    'Reference Date Format' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE,
    'REF_DATE does not match YYYY-MM format' AS failure_reason
FROM housing_starts
WHERE REF_DATE NOT REGEXP '^[0-9]{4}-[0-9]{2}$';

-- ── DQ-010: Grain Uniqueness ────────────────────────────────
-- Dimension: Uniqueness | Severity: High
-- Business Rule: Each combination of the 4 grain columns must be unique.
SELECT
    'DQ-010' AS rule_id,
    'Grain Uniqueness' AS rule_name,
    'Uniqueness' AS dq_dimension,
    REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET,
    COUNT(*) AS duplicate_count,
    'Duplicate grain combination detected' AS failure_reason
FROM housing_starts
GROUP BY REF_DATE, GEO_CODE, DWELLING_TYPE, INTENDED_MARKET
HAVING COUNT(*) > 1;

-- ── DQ-011: Reference Date Not Future-Dated ─────────────────
-- Dimension: Validity | Severity: Medium | CDE: REF_DATE
-- Business Rule: REF_DATE must not be after the current month.
SELECT
    'DQ-011' AS rule_id,
    'Reference Date Not Future' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE,
    'REF_DATE is a future date' AS failure_reason
FROM housing_starts
WHERE REF_DATE > TO_CHAR(CURRENT_DATE, 'YYYY-MM');

-- ── DQ-012: Status Code Validity ────────────────────────────
-- Dimension: Validity | Severity: Low | Column: STATUS
-- Business Rule: STATUS must be one of: blank, 'E', 'F', 'r'
SELECT
    'DQ-012' AS rule_id,
    'Status Code Validity' AS rule_name,
    'Validity' AS dq_dimension,
    REF_DATE, GEO_CODE, STATUS,
    'STATUS is not a recognized quality code' AS failure_reason
FROM housing_starts
WHERE STATUS NOT IN ('', 'E', 'F', 'r')
  AND STATUS IS NOT NULL;

-- ── SUMMARY SCORECARD QUERY ──────────────────────────────────
-- Returns overall pass rate per DQ dimension
SELECT
    dq_dimension,
    COUNT(*) AS rules_executed,
    ROUND(AVG(pass_rate_pct), 2) AS avg_pass_rate,
    SUM(records_failed) AS total_failed_records
FROM dq_rule_results
GROUP BY dq_dimension
ORDER BY avg_pass_rate ASC;
