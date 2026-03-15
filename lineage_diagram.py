"""
=============================================================
CMHC Housing Data Governance Project
Script: lineage_diagram.py
Author: Ram Krishna Dhakal
Purpose: Generates a visual end-to-end data lineage diagram
         showing the 5-layer pipeline from source systems
         to consumption. Saved as PNG for GitHub and reports.
=============================================================
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

OUTPUT_PATH = "docs/data_lineage_diagram.png"

# ── COLOUR PALETTE ────────────────────────────────────────────────────────────
COLORS = {
    "source"      : "#1F4E79",
    "ingest"      : "#2E75B6",
    "process"     : "#C55A11",
    "publish"     : "#538135",
    "consume"     : "#7030A0",
    "arrow"       : "#444444",
    "bg"          : "#F4F6F9",
    "white"       : "#FFFFFF",
    "text_light"  : "#FFFFFF",
    "text_dark"   : "#2C3E50",
    "cde_line"    : "#E74C3C",
    "border"      : "#CCCCCC",
}

def draw_box(ax, x, y, w, h, label, sublabel, color, fontsize=9):
    """Draw a rounded rectangle node with label."""
    box = FancyBboxPatch(
        (x - w/2, y - h/2), w, h,
        boxstyle="round,pad=0.02",
        facecolor=color, edgecolor=COLORS["white"],
        linewidth=1.5, zorder=3
    )
    ax.add_patch(box)
    ax.text(x, y + 0.04, label, ha="center", va="center",
            fontsize=fontsize, fontweight="bold",
            color=COLORS["text_light"], zorder=4, wrap=True,
            multialignment="center")
    if sublabel:
        ax.text(x, y - 0.12, sublabel, ha="center", va="center",
                fontsize=6.5, color=COLORS["text_light"],
                alpha=0.85, zorder=4, multialignment="center")

def draw_arrow(ax, x1, y1, x2, y2, label="", color=COLORS["arrow"], style="->"):
    """Draw an arrow between two points."""
    ax.annotate("",
        xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(
            arrowstyle=style,
            color=color,
            lw=1.8,
            connectionstyle="arc3,rad=0.0"
        ), zorder=2
    )
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        ax.text(mx, my + 0.06, label, ha="center", va="bottom",
                fontsize=6, color=color, style="italic", zorder=5)

def draw_layer_label(ax, x, y, label, color):
    """Draw vertical layer label on the left."""
    ax.text(x, y, label, ha="center", va="center",
            fontsize=8, fontweight="bold", color=color,
            rotation=90,
            bbox=dict(boxstyle="round,pad=0.3", facecolor=color,
                      edgecolor="none", alpha=0.15))

# ── MAIN DIAGRAM ──────────────────────────────────────────────────────────────
def generate_diagram():
    fig, ax = plt.subplots(figsize=(18, 10))
    fig.patch.set_facecolor(COLORS["bg"])
    ax.set_facecolor(COLORS["bg"])
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 10)
    ax.axis("off")

    # ── TITLE ─────────────────────────────────────────────────────────────────
    ax.text(9, 9.5, "End-to-End Data Lineage — CMHC Housing Starts 2018–2023",
            ha="center", va="center", fontsize=14, fontweight="bold",
            color=COLORS["text_dark"])
    ax.text(9, 9.1, "Canadian Housing Data Governance & Quality Framework  |  Author: Ram Krishna Dhakal",
            ha="center", va="center", fontsize=9, color="#7F8C8D")

    # ── LAYER BACKGROUND BANDS ────────────────────────────────────────────────
    layer_bands = [
        (0.2,  2.8,  "#1F4E79", "LAYER 1\nSOURCE"),
        (3.0,  4.8,  "#2E75B6", "LAYER 2\nINGESTION"),
        (5.0,  8.8,  "#C55A11", "LAYER 3\nPROCESSING"),
        (9.0, 11.8,  "#538135", "LAYER 4\nPUBLICATION"),
        (12.0, 17.8, "#7030A0", "LAYER 5\nCONSUMPTION"),
    ]
    for x1, x2, color, label in layer_bands:
        band = FancyBboxPatch(
            (x1, 0.5), x2 - x1, 8.2,
            boxstyle="round,pad=0.1",
            facecolor=color, edgecolor="none",
            alpha=0.08, zorder=0
        )
        ax.add_patch(band)
        ax.text((x1+x2)/2, 8.55, label, ha="center", va="center",
                fontsize=7.5, fontweight="bold", color=color,
                multialignment="center")

    # ── LAYER 1: SOURCE SYSTEMS ───────────────────────────────────────────────
    draw_box(ax, 1.5, 6.8, 2.2, 0.7,
             "Municipal Building\nPermit Offices",
             "Permit applications\nfrom developers",
             COLORS["source"], fontsize=8)

    draw_box(ax, 1.5, 5.2, 2.2, 0.7,
             "CMHC Field\nSurveyor Network",
             "Monthly direct surveys\nof builders",
             COLORS["source"], fontsize=8)

    draw_box(ax, 1.5, 3.6, 2.2, 0.7,
             "CMHC Housing\nPrice Survey",
             "Transaction-level\nprice data",
             COLORS["source"], fontsize=8)

    # ── LAYER 2: INGESTION ────────────────────────────────────────────────────
    draw_box(ax, 3.9, 5.5, 1.6, 1.8,
             "CMHC HMIP\n(Housing Market\nInfo Portal)",
             "Oracle DB\nDeduplication\nStandardization",
             COLORS["ingest"], fontsize=7.5)

    # ── LAYER 3: PROCESSING ───────────────────────────────────────────────────
    draw_box(ax, 6.2, 6.5, 2.2, 0.9,
             "Informatica IDMC\nDQ Engine",
             "12 DQ Rules · Exception Log\nCompleteness · Validity\nUniqueness · Accuracy",
             COLORS["process"], fontsize=7.5)

    draw_box(ax, 6.2, 4.8, 2.2, 0.9,
             "Collibra Data\nIntelligence Cloud",
             "Metadata Catalog · CDEs\nData Lineage · Stewardship\n100+ Assets Documented",
             COLORS["process"], fontsize=7.5)

    draw_box(ax, 6.2, 3.1, 2.2, 0.7,
             "Python DQ Engine\n(dq_engine.py)",
             "Open-source implementation\nof DQ rule framework",
             COLORS["process"], fontsize=7.5)

    # ── LAYER 4: PUBLICATION ──────────────────────────────────────────────────
    draw_box(ax, 10.4, 5.5, 2.0, 1.4,
             "Statistics Canada\nCODR",
             "Public Open Data\nCSV · JSON · SDMX\nMonthly Release",
             COLORS["publish"], fontsize=7.5)

    # ── LAYER 5: CONSUMPTION ──────────────────────────────────────────────────
    draw_box(ax, 14.0, 7.0, 2.4, 0.85,
             "Power BI Dashboard",
             "Housing Starts KPI\nProvincial Trends\nAffordability Indicators",
             COLORS["consume"], fontsize=7.5)

    draw_box(ax, 14.0, 5.5, 2.4, 0.85,
             "Federal Housing\nPolicy Reports",
             "Treasury Board Reporting\nMinister of Housing\nAnnual Supply Report",
             COLORS["consume"], fontsize=7.5)

    draw_box(ax, 14.0, 4.0, 2.4, 0.85,
             "Data Science &\nAnalytics Teams",
             "Ad-hoc Analysis\nML Models\nResearch Publications",
             COLORS["consume"], fontsize=7.5)

    draw_box(ax, 14.0, 2.5, 2.4, 0.7,
             "Public / Open Data\nConsumers",
             "Researchers · Journalists\nReal Estate Industry",
             COLORS["consume"], fontsize=7.5)

    # ── ARROWS: SOURCE → INGEST ───────────────────────────────────────────────
    draw_arrow(ax, 2.6, 6.8, 3.1, 6.0, "Permit records")
    draw_arrow(ax, 2.6, 5.2, 3.1, 5.5, "Survey data")
    draw_arrow(ax, 2.6, 3.6, 3.1, 4.8, "Price data")

    # ── ARROWS: INGEST → PROCESSING ──────────────────────────────────────────
    draw_arrow(ax, 4.7, 6.2, 5.1, 6.5, "Staged data")
    draw_arrow(ax, 4.7, 5.5, 5.1, 4.8, "Metadata")
    draw_arrow(ax, 4.7, 4.8, 5.1, 3.1, "Raw records")

    # ── ARROWS: PROCESSING ───────────────────────────────────────────────────
    draw_arrow(ax, 7.3, 6.5, 7.3, 5.25, "Validated\nassets", color="#C55A11")
    draw_arrow(ax, 7.3, 4.35, 7.3, 3.45, "Rule\nresults", color="#C55A11")

    # ── ARROWS: PROCESSING → PUBLISH ─────────────────────────────────────────
    draw_arrow(ax, 7.3, 6.05, 9.4, 5.9, "DQ-validated data")
    draw_arrow(ax, 7.3, 4.35, 9.4, 5.2, "Approved metadata")

    # ── ARROWS: PUBLISH → CONSUME ────────────────────────────────────────────
    draw_arrow(ax, 11.4, 6.2, 12.8, 7.0, "Monthly feed")
    draw_arrow(ax, 11.4, 5.5, 12.8, 5.5, "Annual data")
    draw_arrow(ax, 11.4, 5.0, 12.8, 4.0, "API / CSV")
    draw_arrow(ax, 11.4, 4.8, 12.8, 2.5, "Open data")

    # ── CDE LINEAGE CALLOUT ───────────────────────────────────────────────────
    ax.text(9.0, 1.8,
            "Critical Data Elements (CDEs) traced end-to-end:  "
            "HOUSING_STARTS  ·  AVERAGE_PRICE_CAD  ·  REF_DATE  ·  GEO  ·  DWELLING_TYPE  ·  INTENDED_MARKET",
            ha="center", va="center", fontsize=8, color=COLORS["cde_line"],
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="#FDE8E8",
                      edgecolor=COLORS["cde_line"], alpha=0.9))

    # ── LEGEND ────────────────────────────────────────────────────────────────
    legend_items = [
        mpatches.Patch(color=COLORS["source"],  label="Layer 1 — Source Systems"),
        mpatches.Patch(color=COLORS["ingest"],  label="Layer 2 — Ingestion (ETL)"),
        mpatches.Patch(color=COLORS["process"], label="Layer 3 — Processing (DQ + Governance)"),
        mpatches.Patch(color=COLORS["publish"], label="Layer 4 — Publication"),
        mpatches.Patch(color=COLORS["consume"], label="Layer 5 — Consumption"),
    ]
    ax.legend(handles=legend_items, loc="lower left",
              bbox_to_anchor=(0.01, 0.01), fontsize=8,
              framealpha=0.9, edgecolor=COLORS["border"])

    # ── FOOTER ────────────────────────────────────────────────────────────────
    ax.text(9, 0.25,
            "github.com/rkdhakal/cmhc-housing-data-governance  |  Ram Krishna Dhakal  |  Data Governance & Quality Framework",
            ha="center", va="center", fontsize=7.5, color="#95A5A6")

    # ── SAVE ──────────────────────────────────────────────────────────────────
    os.makedirs("docs", exist_ok=True)
    plt.tight_layout(pad=0.5)
    plt.savefig(OUTPUT_PATH, dpi=180, bbox_inches="tight",
                facecolor=COLORS["bg"], edgecolor="none")
    plt.close()
    print(f"  ✓ Lineage diagram saved to: {OUTPUT_PATH}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  CMHC Data Lineage Diagram Generator")
    print("  Author: Ram Krishna Dhakal")
    print("=" * 60)
    generate_diagram()
    print("\n  ✅ Done! Open docs/data_lineage_diagram.png to view.")
    print("=" * 60)
