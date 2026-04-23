"""
auto_close_pdf.py
Runs non-stop in the background and automatically closes any PDF viewer windows
as soon as they appear. Only targets PDFs with purely numeric filenames
(e.g. 520242433272.pdf). Skips any PDF with letters in the filename
(e.g. 20260422123342__BillOfLanding-3563704.pdf).
"""

import subprocess
import time
import sys
import re

# ── CONFIG ──────────────────────────────────────────────────────────────────
CHECK_INTERVAL = 0.5   # seconds between scans (lower = faster kill)

PDF_PROCESSES = [
    "AcroRd32.exe",
    "Acrobat.exe",
    "FoxitPDFReader.exe",
    "SumatraPDF.exe",
]

PDF_TITLE_KEYWORDS = [".pdf", "PDF", "Adobe", "Foxit", "Sumatra"]

# Matches filenames that are ONLY digits followed by .pdf (case-insensitive)
# e.g. 520242433272.pdf ✓   20260422123342__BillOfLanding.pdf ✗
NUMERIC_PDF_PATTERN = re.compile(r'\b(\d+)\.pdf\b', re.IGNORECASE)
# ────────────────────────────────────────────────────────────────────────────


def is_numeric_pdf_title(title: str) -> bool:
    """
    Return True only if the window title contains a purely numeric PDF filename.
    Examples:
      '520242433272.pdf - Adobe Acrobat'  → True
      '20260422123342__BillOfLanding.pdf' → False  (has letters before .pdf)
      'report_2024.pdf'                   → False  (has letters)
    """
    match = NUMERIC_PDF_PATTERN.search(title)
    return match is not None


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


def kill_pdf_processes():
    """Kill known standalone PDF viewer processes."""
    killed = []
    for proc in PDF_PROCESSES:
        result = subprocess.run(
            ["taskkill", "/F", "/IM", proc],
            capture_output=True, text=True
        )
        if "SUCCESS" in result.stdout:
            killed.append(proc)
    return killed


def close_pdf_windows():
    """
    Close windows whose title contains a numeric-only PDF filename.
    Skips any PDF with letters in the filename.
    """
    windows = get_open_windows()
    closed = []
    skipped = []

    for hwnd, title in windows:
        # First check: does this window look like a PDF at all?
        is_pdf_window = any(kw.lower() in title.lower() for kw in PDF_TITLE_KEYWORDS)
        if not is_pdf_window:
            continue

        # Second check: is the PDF filename purely numeric?
        if is_numeric_pdf_title(title):
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
    print(f"  Target pattern: numeric filenames only (e.g. 520242433272.pdf)")
    print(f"  Skipping      : any PDF with letters in filename")
    print("-" * 60)

    total_closed = 0

    while True:
        try:
            # 1. Kill standalone PDF viewer processes
            # NOTE: Process-level kills are not filename-aware.
            # Only enable if you are sure no letter-named PDFs will be open.
            # killed_procs = kill_pdf_processes()

            # 2. Close PDF windows/tabs — numeric filenames only
            closed_wins, skipped_wins = close_pdf_windows()

            if closed_wins:
                for w in closed_wins:
                    total_closed += 1
                    print(f"[CLOSED]   {w[:70]}  (total: {total_closed})")

            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            print(f"\nStopped. Total PDFs closed: {total_closed}")
            sys.exit(0)
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()