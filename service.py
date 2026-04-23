"""
services.py
-----------
Service functions called after successful label print jobs.

Main function: save_orders(group_pdfs, report_rows)
  - Takes the group_pdfs dict {master_tracking: [pdf_paths]} from main.py
    and the full report_rows list
  - Groups all tracking numbers per master tracking + PO combination
  - Merges tracking numbers into a comma-separated string
  - Saves one row per order to the `orders` table in order_management DB

Logic per order:
  - PO exists + same master tracking  → SKIP (already up to date)
  - PO exists + different master tracking → UPDATE (new shipment for same PO)
  - PO not found                      → INSERT new row

Column mapping (0-based index in tracking.csv):
  1  = Master Tracking Number
  3  = Tracking Number
  5  = Account Number       (Package Payor Account Number)
  12 = PO Number            (Package P.O. Number)
  13 = Group SKU            (Package Invoice Number)
  18 = State
  19 = Zipcode
  20 = Phone
  21 = Address 1
  22 = Address 2
  23 = Contact Name
"""

from __future__ import annotations
from collections import defaultdict
from db import get_connection


def save_orders(group_pdfs: dict, report_rows: list) -> None:
    """
    Called after all print jobs complete.
    Saves one order row per master tracking number to the orders table.

    :param group_pdfs:   {master_tracking: [stamped_pdf_paths]} — only successfully printed groups
    :param report_rows:  full list of row dicts from load_tracking_report()
    """

    # Only process master tracking numbers that were successfully printed
    printed_masters = set(group_pdfs.keys())

    # Build a lookup: master_tracking -> list of row dicts
    master_rows: dict[str, list] = defaultdict(list)
    for row in report_rows:
        if row["master_tracking"] in printed_masters:
            master_rows[row["master_tracking"]].append(row)

    if not master_rows:
        print("  [DB] No orders to save.")
        return

    conn   = get_connection()
    cursor = conn.cursor()

    # Check if PO exists and return its current master tracking number
    check_sql = """
        SELECT id, master_tracking_number
        FROM orders
        WHERE po_number = %s
        LIMIT 1
    """

    insert_sql = """
        INSERT INTO orders (
            customer,
            po_number,
            master_tracking_number,
            tracking_number,
            status,
            group_sku,
            contact_name,
            address_1,
            address_2,
            zipcode,
            phone,
            city,
            state,
            account_number
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    update_sql = """
        UPDATE orders SET
            customer               = %s,
            master_tracking_number = %s,
            tracking_number        = %s,
            status                 = %s,
            group_sku              = %s,
            contact_name           = %s,
            address_1              = %s,
            address_2              = %s,
            zipcode                = %s,
            phone                  = %s,
            city                   = %s,
            state                  = %s,
            account_number         = %s,
            updated_at             = CURRENT_TIMESTAMP
        WHERE po_number = %s
    """

    saved   = 0
    updated = 0
    skipped = 0
    failed  = 0

    for master_tracking, rows in master_rows.items():
        try:
            first = rows[0]

            tracking_numbers = ", ".join(r["tracking_num"] for r in rows)
            po_number        = first.get("po_number", "")
            group_sku        = first.get("group_sku", "")
            contact_name     = first.get("contact_name", "")
            address_1        = first.get("address_1", "")
            address_2        = first.get("address_2", "") or None
            zipcode          = first.get("zipcode", "")
            phone            = first.get("phone", "")
            state            = first.get("state", "")
            account_number   = first.get("account_number", "")

            cursor.execute(check_sql, (po_number,))
            existing = cursor.fetchone()

            if existing:
                existing_id      = existing[0]
                existing_master  = existing[1]

                if existing_master == master_tracking:
                    # Same PO + same master tracking → skip
                    print(f"  [DB] SKIP    master={master_tracking}  PO={po_number}: already exists")
                    skipped += 1
                    continue
                else:
                    # Same PO but different master tracking → update
                    cursor.execute(update_sql, (
                        contact_name,
                        master_tracking,
                        tracking_numbers,
                        "printed",
                        group_sku,
                        contact_name,
                        address_1,
                        address_2,
                        zipcode,
                        phone,
                        None,           # city — not in CSV
                        state,
                        account_number,
                        po_number,      # WHERE po_number = %s
                    ))
                    print(f"  [DB] Updated master={master_tracking}  PO={po_number}  "
                          f"(prev master={existing_master})")
                    updated += 1

            else:
                # PO not found → insert new row
                cursor.execute(insert_sql, (
                    contact_name,
                    po_number,
                    master_tracking,
                    tracking_numbers,
                    "printed",
                    group_sku,
                    contact_name,
                    address_1,
                    address_2,
                    zipcode,
                    phone,
                    None,           # city — not in CSV
                    state,
                    account_number,
                ))
                print(f"  [DB] Inserted master={master_tracking}  PO={po_number}  "
                      f"tracking_count={len(rows)}")
                saved += 1

        except Exception as e:
            print(f"  [DB] ERROR master={master_tracking}: {e}")
            failed += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n  [DB] Done. Inserted: {saved}  |  Updated: {updated}  |  Skipped: {skipped}  |  Failed: {failed}")