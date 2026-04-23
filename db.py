"""
db.py
-----
MySQL connection helper for myproject using SSL CA certificate.

Requirements: pip install mysql-connector-python

Place ca-certificate.crt in the same folder as this file,
or update DB_SSL_CA to the full path.
"""

import os
import sys
import mysql.connector
from mysql.connector import Error
from config import *



def get_connection():
    """
    Return a MySQL connection to myproject using SSL CA certificate.
    Raises SystemExit if the cert file is missing or connection fails.
    """
    if not os.path.exists(DB_SSL_CA):
        print(f"ERROR: SSL certificate not found: {DB_SSL_CA}")
        sys.exit(1)

    try:
        conn = mysql.connector.connect(
            host            = DB_HOST,
            port            = DB_PORT,
            database        = DB_NAME,
            user            = DB_USER,
            password        = DB_PASSWORD,
            # ssl_ca          = DB_SSL_CA,
            ssl_verify_cert = False,
        )
        return conn
    except Error as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)


# ── USAGE EXAMPLE ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    conn = get_connection()
    print(f"Connected to '{DB_NAME}' on {DB_HOST}")

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT 1 AS ok")
    row = cursor.fetchone()
    print(f"Test query result: {row}")

    cursor.close()
    conn.close()
    print("Connection closed.")