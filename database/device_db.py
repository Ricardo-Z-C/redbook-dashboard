import sqlite3
import time
import os

DEVICES_DB_PATH = "database/devices.db"

def get_device_connection():
    os.makedirs(os.path.dirname(DEVICES_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DEVICES_DB_PATH)
    
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Table: mobile_devices
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mobile_devices (
            device_id TEXT PRIMARY KEY,
            alias TEXT,
            platform TEXT DEFAULT 'Android',
            status TEXT DEFAULT 'Offline',
            last_active_ts INTEGER,
            account_name TEXT
        )
    """)
    
    # Table: comment_tasks
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comment_tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            note_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            comment_content TEXT,
            status TEXT DEFAULT 'Pending',
            create_time INTEGER,
            update_time INTEGER,
            error_msg TEXT
        )
    """)
    
    conn.commit()
    return conn

def register_device(device_id, platform="Android", status="Online", alias=None):
    conn = get_device_connection()
    ts = int(time.time() * 1000)
    
    # Check if exists to preserve alias if not provided
    existing = conn.execute("SELECT alias FROM mobile_devices WHERE device_id = ?", (device_id,)).fetchone()
    current_alias = existing[0] if existing else (alias or f"Device-{device_id[-4:]}")
    
    conn.execute("""
        INSERT OR REPLACE INTO mobile_devices (device_id, alias, platform, status, last_active_ts)
        VALUES (?, ?, ?, ?, ?)
    """, (device_id, current_alias, platform, status, ts))
    conn.commit()
    conn.close()

def get_online_devices():
    conn = get_device_connection()
    # Consider devices active within last 30 seconds as online
    threshold = int(time.time() * 1000) - 30000
    cursor = conn.execute("""
        SELECT device_id, alias, platform, status 
        FROM mobile_devices 
        WHERE last_active_ts > ? OR status = 'Online'
    """, (threshold,))
    devices = [{"id": r[0], "alias": r[1], "platform": r[2], "status": r[3]} for r in cursor.fetchall()]
    conn.close()
    return devices

def create_comment_task(note_id, device_id, content):
    conn = get_device_connection()
    ts = int(time.time() * 1000)
    cursor = conn.execute("""
        INSERT INTO comment_tasks (note_id, device_id, comment_content, status, create_time, update_time)
        VALUES (?, ?, ?, 'Pending', ?, ?)
    """, (note_id, device_id, content, ts, ts))
    task_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return task_id

def get_pending_tasks(device_id):
    conn = get_device_connection()
    cursor = conn.execute("""
        SELECT task_id, note_id, comment_content 
        FROM comment_tasks 
        WHERE device_id = ? AND status = 'Pending'
        ORDER BY create_time ASC
    """, (device_id,))
    tasks = [{"task_id": r[0], "note_id": r[1], "content": r[2]} for r in cursor.fetchall()]
    conn.close()
    return tasks

def update_task_status(task_id, status, error_msg=None):
    conn = get_device_connection()
    ts = int(time.time() * 1000)
    conn.execute("""
        UPDATE comment_tasks 
        SET status = ?, update_time = ?, error_msg = ?
        WHERE task_id = ?
    """, (status, ts, error_msg, task_id))
    conn.commit()
    conn.close()
