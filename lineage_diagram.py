import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import os

OUTPUT_PATH = "docs/data_lineage_diagram.png"

COLORS = {
    "source" : "#1F4E79",
    "ingest" : "#2E75B6",
    "process": "#C55A11",
    "publish": "#538135",
    "consume": "#7030A0",
    "bg"     : "#F4F6F9",
    "white"  : "#FFFFFF",
    "cde"    : "#E74C3C",
    "border" : "#CCCCCC",
}

def box(ax, cx, cy, w, h, title, lines, color):
    rect = FancyBboxPatch(
        (cx - w/2, cy - h/2), w, h,
        boxstyle="round,pad=0.03",
        facecolor=color, edgecolor="#FFFFFF",
        linewidth=1.8, zorder=3
    )
    ax.add_patch(rect)
    all_lines = [title] + lines
    n = len(all_lines)
    step = h / (n + 1)
    for i, line in enumerate(all_lines):
        y = cy + h/2 - step*(i+1)
        bold = (i == 0)
        fs = 8.5 if bold else 7.2
        ax.text(cx, y, line,
                ha="center", va="center",
                fontsize=fs, fontweight="bold" if bold else "normal",
                color="#FFFFFF", zorder=4)

def arrow(ax, x1, y1, x2, y2, label="", color="#444"):
    ax.annotate("",
        xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle="->", color=color,
                        lw=1.6, connectionstyle="arc3,rad=0.0"),
        zorder=2)
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2 + 0.08
        ax.text(mx, my, label, ha="center", va="bottom",
                fontsize=6.5, color=color, style="italic", zorder=5)

fig, ax = plt.subplots(figsize=(24, 13))
fig.patch.set_facecolor(COLORS["bg"])
ax.set_facecolor(COLORS["bg"])
ax.set_xlim(0, 24)
ax.set_ylim(0, 13)
ax.axis("off")

# TITLE
ax.text(12, 12.4, "End-to-End Data Lineage — CMHC Housing Starts 2018–2023",
        ha="center", va="center", fontsize=16, fontweight="bold", color="#2C3E50")
ax.text(12, 11.9, "Canadian Housing Data Governance & Quality Framework  |  Author: Ram Krishna Dhakal",
        ha="center", va="center", fontsize=10, color="#7F8C8D")

# LAYER BANDS
bands = [
    (0.1,   4.1,  "#1F4E79", "LAYER 1\nSOURCE"),
    (4.3,   6.9,  "#2E75B6", "LAYER 2\nINGESTION"),
    (7.1,  12.9,  "#C55A11", "LAYER 3\nPROCESSING"),
    (13.1, 15.9,  "#538135", "LAYER 4\nPUBLICATION"),
    (16.1, 23.9,  "#7030A0", "LAYER 5\nCONSUMPTION"),
]
for x1, x2, c, lbl in bands:
    ax.add_patch(FancyBboxPatch(
        (x1, 0.5), x2-x1, 10.8,
        boxstyle="round,pad=0.1",
        facecolor=c, edgecolor="none", alpha=0.09, zorder=0))
    ax.text((x1+x2)/2, 11.2, lbl,
            ha="center", va="center", fontsize=8.5, fontweight="bold",
            color=c, multialignment="center")

# LAYER 1: SOURCE
box(ax, 2.1, 9.2, 3.4, 1.4,
    "Municipal Building Permit Offices",
    ["Raw permit applications", "from developers & contractors"],
    COLORS["source"])

box(ax, 2.1, 6.5, 3.4, 1.4,
    "CMHC Field Surveyor Network",
    ["Monthly direct surveys", "of builders & developers"],
    COLORS["source"])

box(ax, 2.1, 3.8, 3.4, 1.4,
    "CMHC Housing Price Survey",
    ["Transaction-level price data", "per dwelling type & region"],
    COLORS["source"])

# LAYER 2: INGESTION
box(ax, 5.6, 6.5, 2.4, 4.6,
    "CMHC HMIP",
    ["Housing Market",
     "Info Portal",
     "─────────────",
     "Oracle Database",
     "Deduplication",
     "Standardization",
     "Consolidation"],
    COLORS["ingest"])

# LAYER 3: PROCESSING
box(ax, 10.0, 9.5, 5.2, 1.8,
    "Informatica IDMC — DQ Engine",
    ["12 DQ Rules: Completeness · Validity · Uniqueness",
     "Exception Log Generated · DQ Scorecard Produced",
     "Failed Records Flagged for Steward Review"],
    COLORS["process"])

box(ax, 10.0, 6.5, 5.2, 1.8,
    "Collibra Data Intelligence Cloud",
    ["Metadata Catalog · CDE Documentation",
     "Data Lineage Mapping · Stewardship Workflows",
     "100+ Critical Data Assets Documented"],
    COLORS["process"])

box(ax, 10.0, 3.5, 5.2, 1.8,
    "Python DQ Engine  (dq_engine.py)",
    ["Open-source DQ rule framework implementation",
     "data_profiler.py · lineage_diagram.py",
     "Mirrors Informatica IDMC workflow in Python"],
    COLORS["process"])

# LAYER 4: PUBLICATION
box(ax, 14.5, 6.5, 2.4, 4.6,
    "Statistics Canada",
    ["CODR Repository",
     "─────────────",
     "Public Open Data",
     "CSV · JSON",
     "SDMX Format",
     "Monthly Release"],
    COLORS["publish"])

# LAYER 5: CONSUMPTION
box(ax, 20.0, 9.8, 3.6, 1.4,
    "Power BI Dashboard",
    ["Housing Starts KPIs · Provincial Trends",
     "Affordability Indicators"],
    COLORS["consume"])

box(ax, 20.0, 7.8, 3.6, 1.4,
    "Federal Housing Policy Reports",
    ["Treasury Board · Minister of Housing",
     "Annual Housing Supply Report"],
    COLORS["consume"])

box(ax, 20.0, 5.8, 3.6, 1.4,
    "Data Science & Analytics Teams",
    ["Ad-hoc Analysis · ML Models",
     "Research Publications"],
    COLORS["consume"])

box(ax, 20.0, 3.8, 3.6, 1.4,
    "Public / Open Data Consumers",
    ["Researchers · Journalists",
     "Real Estate Industry"],
    COLORS["consume"])

# ARROWS: SOURCE → INGEST
arrow(ax, 3.8, 9.2,  4.4, 8.2,  "Permit records")
arrow(ax, 3.8, 6.5,  4.4, 6.5,  "Survey data")
arrow(ax, 3.8, 3.8,  4.4, 4.8,  "Price data")

# ARROWS: INGEST → PROCESSING
arrow(ax, 6.8, 8.5,  7.4, 9.2,  "Staged data")
arrow(ax, 6.8, 6.5,  7.4, 6.5,  "Metadata")
arrow(ax, 6.8, 4.5,  7.4, 3.8,  "Raw records")

# ARROWS: WITHIN PROCESSING
arrow(ax, 10.0, 8.6,  10.0, 7.4,  "Validated assets", color=COLORS["process"])
arrow(ax, 10.0, 5.6,  10.0, 4.4,  "Rule results",     color=COLORS["process"])

# ARROWS: PROCESSING → PUBLISH
arrow(ax, 12.6, 9.0,  13.3, 8.2,  "DQ-validated data")
arrow(ax, 12.6, 6.5,  13.3, 6.5,  "Approved metadata")

# ARROWS: PUBLISH → CONSUME
arrow(ax, 15.7, 8.5,  18.2, 9.8,  "Monthly feed")
arrow(ax, 15.7, 7.2,  18.2, 7.8,  "Annual data")
arrow(ax, 15.7, 5.8,  18.2, 5.8,  "API / CSV")
arrow(ax, 15.7, 4.8,  18.2, 3.8,  "Open data")

# CDE BANNER
ax.text(12, 2.0,
    "Critical Data Elements (CDEs) traced end-to-end:   "
    "HOUSING_STARTS  ·  AVERAGE_PRICE_CAD  ·  REF_DATE  ·  GEO  ·  DWELLING_TYPE  ·  INTENDED_MARKET",
    ha="center", va="center", fontsize=9, color=COLORS["cde"],
    fontweight="bold",
    bbox=dict(boxstyle="round,pad=0.5", facecolor="#FDE8E8",
              edgecolor=COLORS["cde"], alpha=0.95, linewidth=1.5))

# LEGEND
legend_items = [
    mpatches.Patch(color=COLORS["source"],  label="Layer 1 — Source Systems"),
    mpatches.Patch(color=COLORS["ingest"],  label="Layer 2 — Ingestion (ETL)"),
    mpatches.Patch(color=COLORS["process"], label="Layer 3 — Processing (DQ + Governance)"),
    mpatches.Patch(color=COLORS["publish"], label="Layer 4 — Publication"),
    mpatches.Patch(color=COLORS["consume"], label="Layer 5 — Consumption"),
]
ax.legend(handles=legend_items, loc="lower left",
          bbox_to_anchor=(0.005, 0.005), fontsize=9,
          framealpha=0.95, edgecolor=COLORS["border"], fancybox=True)

# FOOTER
ax.text(12, 0.22,
    "github.com/rkdhakal/cmhc-housing-data-governance  |  "
    "Ram Krishna Dhakal  |  Data Governance & Quality Framework",
    ha="center", va="center", fontsize=8.5, color="#95A5A6")

os.makedirs("docs", exist_ok=True)
plt.savefig(OUTPUT_PATH, dpi=180, bbox_inches="tight",
            facecolor=COLORS["bg"], edgecolor="none")
plt.close()
print(f"Saved: {OUTPUT_PATH}")
