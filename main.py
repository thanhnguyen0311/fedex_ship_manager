"""
main.py
-------
Combined pipeline:
  1. Scan output_label/ folder → collect already-generated tracking numbers (PDF filenames)
  2. Read tracking.csv → get all tracking numbers from the report
  3. Find missing ones (in report but no label yet)
  4. For each missing: match item SKU via dimension lookup
  5. Stamp Group SKU / Item SKU / PO Number onto the source label PDF and save to output_label/
  6. Group by master tracking number → merge into one PDF per group → send as single print job

Requirements:  pip install pandas pypdf fpdf2 pywin32

Folder structure expected:
  main.py
  tracking.csv        ← export report
  dimension.csv       ← dimension reference
  output_label/       ← stamped output (created automatically)
"""

from __future__ import annotations
import os
import io
import sys
import math
import time
import platform
from collections import defaultdict
import pandas as pd
from pypdf import PdfReader, PdfWriter
from fpdf import FPDF
import pdfplumber

# ── CONFIG ────────────────────────────────────────────────────────────────────
TRACKING_CSV     = "tracking.csv"
DIMENSION_CSV    = "dimension.csv"
SOURCE_LABEL_DIR = r"C:\ProgramData\FedEx\FSM\Temp"  # original FedEx label PDFs
OUTPUT_LABEL_DIR = "output_label"                     # stamped output
TOLERANCE        = 0.5    # dimension match tolerance (whole numbers after floor)
FONT_SIZE        = 11     # text size for all stamped fields
PRINT_DELAY      = 5    # seconds to wait between print jobs
# ─────────────────────────────────────────────────────────────────────────────

# Item slot column indices (0-based) in dimension.csv
ITEM_SLOTS = [
    (2,  4,  5,  6),    # Item 1: name=C, L=E, W=F, H=G
    (8,  10, 11, 12),   # Item 2: name=I, L=K, W=L, H=M
    (14, 16, 17, 18),   # Item 3: name=O, L=Q, W=R, H=S
    (20, 22, 23, 24),   # Item 4: name=U, L=W, W=X, H=Y
]

# ── Label stamp positions (points, pdfplumber top-down coords) ────────────────
SKU_X_PT    = 101.7 + 6;  SKU_TOP_PT  = 262.7;  SKU_BOT_PT  = 275.7
GSKU_X_PT   = 83.4  + 6;  GSKU_TOP_PT = 282.1;  GSKU_BOT_PT = 292.1
PO_X_PT     = 22.0;       PO_TOP_PT   = 135.4;  PO_BOT_PT   = 142.4
# ─────────────────────────────────────────────────────────────────────────────


# ── STEP 1: Scan output_label folder ─────────────────────────────────────────

def get_existing_labels(folder):
    if not os.path.exists(folder):
        return set()
    existing = set()
    for fname in os.listdir(folder):
        if fname.lower().endswith(".pdf"):
            existing.add(os.path.splitext(fname)[0])
    return existing


# ── STEP 2: Read tracking report ─────────────────────────────────────────────

def load_tracking_report(csv_path):
    """
    Column mapping (0-based index):
      B  (1)  = Master Tracking Number
      D  (3)  = Tracking Number
      G  (6)  = Height
      H  (7)  = Width
      I  (8)  = Length
      M  (12) = PO Number (forward-filled within master tracking group)
      N  (13) = Group SKU (forward-filled globally)
    """
    df = pd.read_csv(csv_path, header=0, dtype=str)
    df.iloc[:, 13] = df.iloc[:, 13].replace("", pd.NA).ffill()
    df.iloc[:, 12] = df.iloc[:, 12].replace("", pd.NA)
    df.iloc[:, 12] = df.groupby(df.iloc[:, 1], sort=False)[df.columns[12]].ffill()

    records = []
    for _, row in df.iterrows():
        master_tracking = str(row.iloc[1]).strip()
        tracking_num    = str(row.iloc[3]).strip()
        group_sku       = str(row.iloc[13]).strip()
        po_number       = str(row.iloc[12]).strip()

        try:
            height = math.floor(float(row.iloc[6]))
            width  = math.floor(float(row.iloc[7]))
            length = math.floor(float(row.iloc[8]))
        except (ValueError, TypeError):
            print(f"  SKIP (bad dimensions): {tracking_num}")
            continue

        if not group_sku or group_sku.lower() == "nan":
            print(f"  SKIP (no group SKU): {tracking_num}")
            continue

        records.append({
            "master_tracking": master_tracking,
            "tracking_num":    tracking_num,
            "group_sku":       group_sku,
            "po_number":       po_number if po_number.lower() != "nan" else "",
            "height":          height,
            "width":           width,
            "length":          length,
        })
    return records


# ── STEP 3: Load dimension lookup ────────────────────────────────────────────

def load_dimension_csv(dim_path):
    df = pd.read_csv(dim_path, header=0, dtype=str)
    lookup = {}
    for _, row in df.iterrows():
        group_sku = str(row.iloc[0]).strip()
        if not group_sku or group_sku.lower() == "nan":
            continue
        items = []
        for name_idx, len_idx, wid_idx, hgt_idx in ITEM_SLOTS:
            try:
                item_name = str(row.iloc[name_idx]).strip()
                length    = float(row.iloc[len_idx])
                width     = float(row.iloc[wid_idx])
                height    = float(row.iloc[hgt_idx])
            except (IndexError, ValueError, TypeError):
                continue
            if item_name and item_name.lower() != "nan":
                try:
                    items.append({"item_name": item_name,
                                  "length": math.floor(length),
                                  "width":  math.floor(width),
                                  "height": math.floor(height)})
                except (ValueError, TypeError):
                    continue
        if items:
            lookup[group_sku] = items
    return lookup


# ── STEP 4: Match SKU by dimensions ──────────────────────────────────────────

def dims_match(r_h, r_w, r_l, d_h, d_w, d_l):
    return (abs(r_h - d_h) <= TOLERANCE and
            abs(r_w - d_w) <= TOLERANCE and
            abs(r_l - d_l) <= TOLERANCE)


def find_item_sku(group_sku, height, width, length, lookup):
    if group_sku not in lookup:
        return None, f"group SKU '{group_sku}' not found in dimension.csv"
    for item in lookup[group_sku]:
        if dims_match(height, width, length,
                      item["height"], item["width"], item["length"]):
            return item["item_name"], None
    available = [(i["item_name"], f"L={i['length']} W={i['width']} H={i['height']}")
                 for i in lookup[group_sku]]
    return None, f"no dim match (report H={height} W={width} L={length} | options={available})"


# ── STEP 5: Stamp details onto label PDF ─────────────────────────────────────

def has_existing_po(src_pdf):
    """Return True if the label already has text in the PO: field area.
    Checks for any words between x=21 (after 'PO:') and x=150 (before 'DEPT:')
    on the PO row (top=133..144).
    """
    with pdfplumber.open(src_pdf) as pdf:
        page = pdf.pages[0]
        words = page.extract_words()
        for w in words:
            if 133 < w["top"] < 144 and w["x0"] > 21 and w["x1"] < 150:
                return True
        return False


def make_overlay(item_sku, group_sku, po_number, page_w_pt, page_h_pt):
    PT2MM = 0.352778
    pdf = FPDF(unit="mm", format=(page_w_pt * PT2MM, page_h_pt * PT2MM))
    pdf.add_page()
    pdf.set_font("Helvetica", style="B", size=FONT_SIZE)
    pdf.set_text_color(0, 0, 0)

    def write_field(x_pt, top_pt, bot_pt, text):
        pdf.set_xy(x_pt * PT2MM, top_pt * PT2MM)
        pdf.cell(w=0, h=(bot_pt - top_pt) * PT2MM, text=text)

    write_field(SKU_X_PT,  SKU_TOP_PT,  SKU_BOT_PT,  f"SKU: {item_sku}")
    if group_sku != item_sku:
        write_field(GSKU_X_PT, GSKU_TOP_PT, GSKU_BOT_PT, f"GRP: {group_sku}")
    if po_number:
        write_field(PO_X_PT, PO_TOP_PT, PO_BOT_PT, po_number)

    return pdf.output()


def stamp_label(src_pdf, item_sku, group_sku, po_number, out_pdf):
    reader = PdfReader(src_pdf)
    writer = PdfWriter()
    for page in reader.pages:
        w = float(page.mediabox.width)
        h = float(page.mediabox.height)
        overlay = PdfReader(io.BytesIO(make_overlay(item_sku, group_sku, po_number, w, h)))
        page.merge_page(overlay.pages[0])
        writer.add_page(page)
    with open(out_pdf, "wb") as f:
        writer.write(f)


# ── STEP 6: Merge group PDFs and send single print job ───────────────────────

def merge_pdfs(pdf_paths):
    """Merge a list of PDF paths into one in-memory PDF bytes."""
    writer = PdfWriter()
    for path in pdf_paths:
        reader = PdfReader(path)
        for page in reader.pages:
            writer.add_page(page)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def print_pdf(pdf_path):
    """Send a PDF file to the default printer."""
    if platform.system() == "Windows":
        import win32api
        win32api.ShellExecute(0, "print", pdf_path, None, ".", 0)
    else:
        os.system(f'lpr "{pdf_path}"')


def print_group(master_tracking, pdf_paths):
    """Merge all labels for a master tracking group and send as one print job."""
    merged_bytes = merge_pdfs(pdf_paths)
    # Write merged PDF to a temp file in output_label
    merged_path = os.path.join(OUTPUT_LABEL_DIR, f"_print_{master_tracking}.pdf")
    with open(merged_path, "wb") as f:
        f.write(merged_bytes)
    print_pdf(merged_path)
    print(f"  >> Print job sent: master {master_tracking} "
          f"({len(pdf_paths)} label(s)) → {os.path.basename(merged_path)}")


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    for path in (TRACKING_CSV, DIMENSION_CSV):
        if not os.path.exists(path):
            print(f"ERROR: '{path}' not found.")
            sys.exit(1)
    if not os.path.exists(SOURCE_LABEL_DIR):
        print(f"ERROR: source label folder '{SOURCE_LABEL_DIR}' not found.")
        sys.exit(1)

    os.makedirs(OUTPUT_LABEL_DIR, exist_ok=True)

    # Step 1 — already done labels
    existing = get_existing_labels(OUTPUT_LABEL_DIR)
    print(f"[1] output_label/: {len(existing)} label(s) already generated")

    # Step 2 — all tracking numbers from report
    print(f"[2] Reading '{TRACKING_CSV}' ...")
    report_rows = load_tracking_report(TRACKING_CSV)
    all_tracking = {r["tracking_num"] for r in report_rows}
    print(f"    {len(all_tracking)} tracking number(s) in report")

    # Step 3 — find missing
    missing_nums = all_tracking - existing
    print(f"[3] {len(missing_nums)} label(s) need to be generated\n")

    if not missing_nums:
        print("Nothing to do — all labels already exist.")
        return

    # Step 4 — load dimension lookup
    print(f"[4] Loading '{DIMENSION_CSV}' ...")
    lookup = load_dimension_csv(DIMENSION_CSV)
    print(f"    {len(lookup)} group SKU(s) loaded\n")

    # Step 5 — stamp all missing labels, sorted by master tracking → tracking num
    report_rows.sort(key=lambda r: (r["master_tracking"], r["tracking_num"]))

    success = 0
    failed  = 0
    # Collect stamped PDF paths grouped by master tracking number (preserving order)
    group_pdfs = defaultdict(list)

    print("[5] Stamping labels ...")
    for row in report_rows:
        tnum = row["tracking_num"]
        if tnum not in missing_nums:
            continue

        src_pdf = os.path.join(SOURCE_LABEL_DIR, f"{tnum}.pdf")
        if not os.path.exists(src_pdf):
            print(f"  SKIP  {tnum}: source PDF not found in '{SOURCE_LABEL_DIR}'")
            failed += 1
            continue

        sku, err = find_item_sku(row["group_sku"], row["height"],
                                 row["width"], row["length"], lookup)
        if sku is None:
            print(f"  SKIP  {tnum}: {err}")
            failed += 1
            continue

        out_pdf = os.path.join(OUTPUT_LABEL_DIR, f"{tnum}.pdf")
        po_to_stamp = row["po_number"] if not has_existing_po(src_pdf) else ""
        stamp_label(src_pdf, sku, row["group_sku"], po_to_stamp, out_pdf)
        group_pdfs[row["master_tracking"]].append(out_pdf)
        po_status = f"PO:{row['po_number']}" if po_to_stamp else "PO:already on label"
        print(f"  OK    {tnum}  ->  SKU:{sku}  GRP:{row['group_sku']}  {po_status}")
        success += 1

    # Step 6 — send one print job per master tracking group
    print(f"\n[6] Sending {len(group_pdfs)} print job(s) ...")
    for master_tracking, pdf_paths in group_pdfs.items():
        print_group(master_tracking, pdf_paths)
        time.sleep(PRINT_DELAY)   # small delay between jobs so spooler stays ordered

    print(f"\nDone.  Stamped: {success}  |  Skipped/Failed: {failed}  |  Print jobs: {len(group_pdfs)}")


if __name__ == "__main__":
    main()