"""
auto_close_pdf.py
Runs non-stop in the background and automatically closes any PDF viewer windows
as soon as they appear. Only targets PDFs with purely numeric filenames
(e.g. 520242433272.pdf). Skips any PDF with letters in the filename
(e.g. LABELS-18-18-15.pdf, BillOfLanding.pdf).
"""

import subprocess
import time
import sys
import re

# ── CONFIG ──────────────────────────────────────────────────────────────────
CHECK_INTERVAL = 0.5   # seconds between scans (lower = faster kill)

PDF_TITLE_KEYWORDS = [".pdf", "PDF", "Adobe", "Foxit", "Sumatra"]

# Extracts the PDF filename from a window title (anything ending in .pdf)
PDF_FILENAME_PATTERN = re.compile(r'([^\\/\s]+\.pdf)', re.IGNORECASE)

# The extracted filename must be ENTIRELY digits + .pdf — nothing else
NUMERIC_ONLY_PDF_PATTERN = re.compile(r'^\d+\.pdf$', re.IGNORECASE)
# ────────────────────────────────────────────────────────────────────────────


def is_numeric_only_pdf(title: str) -> bool:
    """
    Extract the PDF filename from the window title, then check if it is
    composed of digits ONLY (no letters, dashes, underscores, etc.).

    ✓  '520242433272.pdf - Adobe Acrobat'   → True   (filename: 520242433272.pdf)
    ✗  'LABELS-18-18-15.pdf - Chrome'       → False  (filename has letters/dashes)
    ✗  '20260422__BillOfLanding.pdf'        → False  (filename has letters)
    ✗  'report_2024.pdf'                    → False  (filename has underscore + letters)
    """
    match = PDF_FILENAME_PATTERN.search(title)
    if not match:
        return False
    filename = match.group(1)                      # e.g. "LABELS-18-18-15.pdf"
    return bool(NUMERIC_ONLY_PDF_PATTERN.match(filename))  # full-string check


def get_open_windows():
    """Return list of (hwnd, title) for all visible windows."""
    try:
        import ctypes
        import ctypes.wintypes

        user32 = ctypes.windll.user32
        results = []

        EnumWindowsProc = ctypes.WINFUNCTYPE(
            ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM
        )

        def callback(hwnd, _):
            if user32.IsWindowVisible(hwnd):
                length = user32.GetWindowTextLengthW(hwnd)
                if length:
                    buf = ctypes.create_unicode_buffer(length + 1)
                    user32.GetWindowTextW(hwnd, buf, length + 1)
                    results.append((hwnd, buf.value))
            return True

        user32.EnumWindows(EnumWindowsProc(callback), 0)
        return results
    except Exception:
        return []


def close_window(hwnd):
    """Send WM_CLOSE to a window handle."""
    try:
        import ctypes
        WM_CLOSE = 0x0010
        ctypes.windll.user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)
    except Exception:
        pass


def close_pdf_windows():
    """
    Close windows whose title contains a purely numeric PDF filename.
    Any PDF filename containing letters, dashes, or underscores is skipped.
    """
    windows = get_open_windows()
    closed = []
    skipped = []

    for hwnd, title in windows:
        # Step 1: Does this window look like a PDF viewer at all?
        is_pdf_window = any(kw.lower() in title.lower() for kw in PDF_TITLE_KEYWORDS)
        if not is_pdf_window:
            continue

        # Step 2: Is the PDF filename purely numeric digits only?
        if is_numeric_only_pdf(title):
            close_window(hwnd)
            closed.append(title)
        else:
            skipped.append(title)

    return closed, skipped


def main():
    print("=" * 60)
    print("  Auto PDF Closer — running (Ctrl+C to stop)")
    print("=" * 60)
    print(f"  Scan interval : {CHECK_INTERVAL}s")
    print(f"  CLOSE  : numeric filenames only  e.g. 520242466859.pdf")
    print(f"  SKIP   : any letters/symbols     e.g. LABELS-18-18-15.pdf")
    print("-" * 60)

    total_closed = 0

    while True:
        try:
            closed_wins, skipped_wins = close_pdf_windows()

            for w in closed_wins:
                total_closed += 1
                print(f"[CLOSED]  {w[:70]}  (total: {total_closed})")

            # for w in skipped_wins:
            #     print(f"[SKIP]    {w[:70]}")

            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            print(f"\nStopped. Total PDFs closed: {total_closed}")
            sys.exit(0)
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()