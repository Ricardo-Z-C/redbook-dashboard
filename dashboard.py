import streamlit as st
import sqlite3
import pandas as pd
import json
import re
import hashlib
import asyncio
from datetime import datetime, timedelta, timezone
import subprocess
import os
import time
import signal
import base64
import sys
import config
import textwrap
import random
import urllib.request
import urllib.error
import urllib.parse
import concurrent.futures
import socket
import threading
import contextlib

# Page configuration
st.set_page_config(
    page_title="小红书电子员工工作台",
    page_icon="📕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Ensure runtime paths are anchored to this file location.
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATABASE_DIR = os.path.abspath(os.getenv("APP_DATABASE_DIR", os.path.join(BASE_DIR, "database")))
CONFIG_DIR = os.path.abspath(os.getenv("APP_CONFIG_DIR", os.path.join(BASE_DIR, "config")))

# Database path
DB_PATH = os.path.join(DATABASE_DIR, "sqlite_tables.db")
TASKS_DB_PATH = os.path.join(DATABASE_DIR, "tasks.db")
ACCOUNTS_FILE = os.path.join(CONFIG_DIR, "accounts.json")
COMMENT_TEMPLATES_FILE = os.path.join(CONFIG_DIR, "comment_templates.json")
COMMENT_STRATEGY_FILE = os.path.join(CONFIG_DIR, "comment_strategy.json")
COMMENT_STRATEGY_EVENT_FILE = os.path.join(CONFIG_DIR, "comment_strategy_events.json")
OPERATION_EXCEPTIONS_FILE = os.path.join(CONFIG_DIR, "operation_exceptions.json")
COMMENT_STRATEGY_PACKAGES_FILE = os.path.join(CONFIG_DIR, "comment_strategy_packages.json")
COMMENT_EXECUTOR_ACCOUNTS_FILE = os.path.join(CONFIG_DIR, "comment_executor_accounts.json")
EMPLOYEE_PROFILES_FILE = os.path.join(CONFIG_DIR, "employee_profiles.json")
EMPLOYEE_PROMPTS_FILE = os.path.join(CONFIG_DIR, "employee_prompts.json")
LLM_RUNTIME_SETTINGS_FILE = os.path.join(CONFIG_DIR, "llm_runtime_settings.json")
MCP_BASE_URL = os.getenv("MCP_BASE_URL", "http://localhost:18060").strip() or "http://localhost:18060"
MCP_DEFAULT_RUNTIME_ACCOUNT_ID = "mcp_runtime"
_MCP_PARSED_URL = urllib.parse.urlparse(MCP_BASE_URL)
MCP_HOST = _MCP_PARSED_URL.hostname or "localhost"
MCP_PORT = _MCP_PARSED_URL.port or 80
MCP_STARTUP_TIMEOUT_SECONDS = 15
MCP_AUTOSTART_LOCK = threading.Lock()
MCP_AUTOSTART_COOKIE_PATH = os.path.abspath(os.path.join(BASE_DIR, "cookies.json"))
MCP_AUTOSTART_COOKIE_STORE_DIR = os.path.abspath(
    os.path.join(BASE_DIR, ".runtime-cookies")
)
MCP_BROWSER_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
]
EMPLOYEE_TYPE_OPTIONS = ["seeding", "lead_generation", "strategy_lead"]
EMPLOYEE_PROMPT_SCOPE_OPTIONS = ["all", "seeding", "lead_generation", "strategy_lead"]
STRATEGY_LEAD_SCOPE_OPTIONS = ["all", "selected"]

COMMENT_JOB_STATUS_LABELS = {
    "queued": "排队中",
    "dispatching": "发送中",
    "sent_api_ok": "接口发送成功",
    "send_failed": "发送失败",
    "verify_pending": "待回溯",
    "verified_visible": "已确认上评",
    "verified_hidden": "未确认上评",
    "manual_review": "需人工复核",
    "cancelled": "已取消",
}
COMMENT_JOB_SUCCESS_STATUSES = {
    "sent_api_ok",
    "verify_pending",
    "verified_visible",
    "verified_hidden",
    "manual_review",
}
COMMENT_JOB_VERIFICATION_READY_STATUSES = {
    "sent_api_ok",
    "verify_pending",
    "verified_hidden",
    "manual_review",
    "verified_visible",
}
COMMENT_JOB_TERMINAL_STATUSES = {
    "send_failed",
    "verified_visible",
    "verified_hidden",
    "manual_review",
    "cancelled",
}
COMMENT_VERIFICATION_VERDICT_LABELS = {
    "visible": "已确认上评",
    "hidden": "未确认上评",
    "manual_review": "需人工复核",
    "failed": "检查失败",
}
OPERATION_EXCEPTION_SOURCE_LABELS = {
    "crawl": "爬取执行",
    "comment_send": "评论发送",
    "comment_verify": "回溯检查",
}
OPERATION_EXCEPTION_SEVERITY_LABELS = {
    "critical": "阻塞",
    "warn": "警告",
    "info": "提示",
}
OPERATION_EXCEPTION_STATUS_LABELS = {
    "open": "待处理",
    "resolved": "已解决",
    "ignored": "已忽略",
}

def get_connection():
    os.makedirs(DATABASE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    
    # Check if account_name column exists in xhs_note (migration)
    try:
        cursor = conn.execute("PRAGMA table_info(xhs_note)")
        columns = [info[1] for info in cursor.fetchall()]
        if "account_name" not in columns:
            conn.execute("ALTER TABLE xhs_note ADD COLUMN account_name TEXT")
        if "comment_status" not in columns:
            conn.execute("ALTER TABLE xhs_note ADD COLUMN comment_status TEXT DEFAULT 'Uncommented'")
        if "dashboard_user" not in columns:
            conn.execute("ALTER TABLE xhs_note ADD COLUMN dashboard_user TEXT")
    except:
        pass
        
    return conn

def get_tasks_connection():
    os.makedirs(DATABASE_DIR, exist_ok=True)
    conn = sqlite3.connect(TASKS_DB_PATH)

    # Ensure tasks table exists and includes required columns.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crawler_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            sort_type TEXT NOT NULL,
            max_count INTEGER NOT NULL,
            start_date TEXT,
            end_date TEXT,
            status TEXT DEFAULT 'Pending',
            last_error TEXT,
            pid INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_employee_kpi_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dashboard_user TEXT NOT NULL,
            employee_id TEXT NOT NULL,
            employee_name_snapshot TEXT,
            employee_type_snapshot TEXT,
            goal_description_snapshot TEXT,
            report_date TEXT NOT NULL,
            target_crawl_count INTEGER DEFAULT 0,
            target_comment_count INTEGER DEFAULT 0,
            risk_level TEXT DEFAULT 'medium',
            reasoning TEXT,
            source_snapshot_json TEXT,
            prompt_id TEXT,
            prompt_updated_at TEXT,
            llm_model TEXT,
            prompt_tokens INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (dashboard_user, employee_id, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_boss_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dashboard_user TEXT NOT NULL,
            report_date TEXT NOT NULL,
            summary_markdown TEXT,
            summary_json TEXT,
            employee_count INTEGER DEFAULT 0,
            total_target_crawl_count INTEGER DEFAULT 0,
            total_target_comment_count INTEGER DEFAULT 0,
            total_actual_note_count INTEGER DEFAULT 0,
            total_sent_comment_count INTEGER DEFAULT 0,
            total_failed_comment_count INTEGER DEFAULT 0,
            llm_model TEXT,
            prompt_tokens INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (dashboard_user, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comment_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dashboard_user TEXT NOT NULL,
            note_id TEXT NOT NULL,
            note_title TEXT,
            note_url TEXT,
            xsec_token TEXT,
            target_type TEXT DEFAULT 'note',
            target_comment_id TEXT,
            target_user_id TEXT,
            target_nickname TEXT,
            target_comment_preview TEXT,
            template_id TEXT,
            template_name TEXT,
            strategy_package_id TEXT,
            strategy_package_name TEXT,
            comment_account_id TEXT,
            comment_account_name TEXT,
            verifier_account_id TEXT,
            verifier_account_name TEXT,
            comment_content TEXT,
            comment_content_hash TEXT,
            status TEXT DEFAULT 'queued',
            scheduled_for TEXT,
            strategy_suggested_for TEXT,
            strategy_delayed INTEGER DEFAULT 0,
            jitter_sec INTEGER DEFAULT 0,
            send_attempt_count INTEGER DEFAULT 0,
            verification_attempt_count INTEGER DEFAULT 0,
            request_id TEXT,
            mcp_username TEXT,
            posted_comment_id TEXT,
            verified_comment_id TEXT,
            verified_comment_content TEXT,
            verified_comment_like_count INTEGER DEFAULT 0,
            verification_reason TEXT,
            last_error TEXT,
            result_message TEXT,
            payload_json TEXT,
            send_response_json TEXT,
            queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            executed_at TEXT,
            last_verified_at TEXT,
            cancelled_at TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comment_verification_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dashboard_user TEXT NOT NULL,
            comment_job_id INTEGER NOT NULL,
            verifier_account_id TEXT,
            verifier_account_name TEXT,
            status TEXT DEFAULT 'pending',
            verdict TEXT,
            matched_comment_id TEXT,
            matched_comment_content TEXT,
            matched_comment_like_count INTEGER DEFAULT 0,
            matched_comment_create_time TEXT,
            matched_by TEXT,
            reason TEXT,
            error_message TEXT,
            result_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            verified_at TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS verifier_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dashboard_user TEXT NOT NULL,
            account_id TEXT NOT NULL,
            account_name TEXT,
            status TEXT DEFAULT 'active',
            is_default INTEGER DEFAULT 0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (dashboard_user, account_id)
        )
        """
        )

    # Migration: add columns introduced later versions.
    try:
        cursor = conn.execute("PRAGMA table_info(crawler_tasks)")
        columns = [info[1] for info in cursor.fetchall()]
        if "account_name" not in columns:
            conn.execute("ALTER TABLE crawler_tasks ADD COLUMN account_name TEXT")
        if "dashboard_user" not in columns:
            conn.execute("ALTER TABLE crawler_tasks ADD COLUMN dashboard_user TEXT")
        if "last_error" not in columns:
            conn.execute("ALTER TABLE crawler_tasks ADD COLUMN last_error TEXT")
        if "filter_condition" not in columns:
            conn.execute("ALTER TABLE crawler_tasks ADD COLUMN filter_condition TEXT")
        if "low_risk_mode" not in columns:
            conn.execute("ALTER TABLE crawler_tasks ADD COLUMN low_risk_mode INTEGER DEFAULT 0")
    except Exception:
        pass

    try:
        cursor = conn.execute("PRAGMA table_info(daily_employee_kpi_snapshots)")
        columns = [info[1] for info in cursor.fetchall()]
        if "source_snapshot_json" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN source_snapshot_json TEXT")
        if "prompt_id" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN prompt_id TEXT")
        if "prompt_updated_at" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN prompt_updated_at TEXT")
        if "llm_model" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN llm_model TEXT")
        if "prompt_tokens" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN prompt_tokens INTEGER DEFAULT 0")
        if "completion_tokens" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN completion_tokens INTEGER DEFAULT 0")
        if "total_tokens" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN total_tokens INTEGER DEFAULT 0")
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE daily_employee_kpi_snapshots ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass

    try:
        cursor = conn.execute("PRAGMA table_info(comment_jobs)")
        columns = [info[1] for info in cursor.fetchall()]
        comment_job_columns = {
            "note_url": "ALTER TABLE comment_jobs ADD COLUMN note_url TEXT",
            "xsec_token": "ALTER TABLE comment_jobs ADD COLUMN xsec_token TEXT",
            "target_type": "ALTER TABLE comment_jobs ADD COLUMN target_type TEXT DEFAULT 'note'",
            "target_comment_id": "ALTER TABLE comment_jobs ADD COLUMN target_comment_id TEXT",
            "target_user_id": "ALTER TABLE comment_jobs ADD COLUMN target_user_id TEXT",
            "target_nickname": "ALTER TABLE comment_jobs ADD COLUMN target_nickname TEXT",
            "target_comment_preview": "ALTER TABLE comment_jobs ADD COLUMN target_comment_preview TEXT",
            "template_id": "ALTER TABLE comment_jobs ADD COLUMN template_id TEXT",
            "template_name": "ALTER TABLE comment_jobs ADD COLUMN template_name TEXT",
            "strategy_package_id": "ALTER TABLE comment_jobs ADD COLUMN strategy_package_id TEXT",
            "strategy_package_name": "ALTER TABLE comment_jobs ADD COLUMN strategy_package_name TEXT",
            "comment_account_id": "ALTER TABLE comment_jobs ADD COLUMN comment_account_id TEXT",
            "comment_account_name": "ALTER TABLE comment_jobs ADD COLUMN comment_account_name TEXT",
            "verifier_account_id": "ALTER TABLE comment_jobs ADD COLUMN verifier_account_id TEXT",
            "verifier_account_name": "ALTER TABLE comment_jobs ADD COLUMN verifier_account_name TEXT",
            "comment_content": "ALTER TABLE comment_jobs ADD COLUMN comment_content TEXT",
            "comment_content_hash": "ALTER TABLE comment_jobs ADD COLUMN comment_content_hash TEXT",
            "status": "ALTER TABLE comment_jobs ADD COLUMN status TEXT DEFAULT 'queued'",
            "scheduled_for": "ALTER TABLE comment_jobs ADD COLUMN scheduled_for TEXT",
            "strategy_suggested_for": "ALTER TABLE comment_jobs ADD COLUMN strategy_suggested_for TEXT",
            "strategy_delayed": "ALTER TABLE comment_jobs ADD COLUMN strategy_delayed INTEGER DEFAULT 0",
            "jitter_sec": "ALTER TABLE comment_jobs ADD COLUMN jitter_sec INTEGER DEFAULT 0",
            "send_attempt_count": "ALTER TABLE comment_jobs ADD COLUMN send_attempt_count INTEGER DEFAULT 0",
            "verification_attempt_count": "ALTER TABLE comment_jobs ADD COLUMN verification_attempt_count INTEGER DEFAULT 0",
            "request_id": "ALTER TABLE comment_jobs ADD COLUMN request_id TEXT",
            "mcp_username": "ALTER TABLE comment_jobs ADD COLUMN mcp_username TEXT",
            "posted_comment_id": "ALTER TABLE comment_jobs ADD COLUMN posted_comment_id TEXT",
            "verified_comment_id": "ALTER TABLE comment_jobs ADD COLUMN verified_comment_id TEXT",
            "verified_comment_content": "ALTER TABLE comment_jobs ADD COLUMN verified_comment_content TEXT",
            "verified_comment_like_count": "ALTER TABLE comment_jobs ADD COLUMN verified_comment_like_count INTEGER DEFAULT 0",
            "verification_reason": "ALTER TABLE comment_jobs ADD COLUMN verification_reason TEXT",
            "last_error": "ALTER TABLE comment_jobs ADD COLUMN last_error TEXT",
            "result_message": "ALTER TABLE comment_jobs ADD COLUMN result_message TEXT",
            "payload_json": "ALTER TABLE comment_jobs ADD COLUMN payload_json TEXT",
            "send_response_json": "ALTER TABLE comment_jobs ADD COLUMN send_response_json TEXT",
            "queued_at": "ALTER TABLE comment_jobs ADD COLUMN queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "started_at": "ALTER TABLE comment_jobs ADD COLUMN started_at TEXT",
            "executed_at": "ALTER TABLE comment_jobs ADD COLUMN executed_at TEXT",
            "last_verified_at": "ALTER TABLE comment_jobs ADD COLUMN last_verified_at TEXT",
            "cancelled_at": "ALTER TABLE comment_jobs ADD COLUMN cancelled_at TEXT",
            "updated_at": "ALTER TABLE comment_jobs ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
        for column_name, statement in comment_job_columns.items():
            if column_name not in columns:
                conn.execute(statement)
    except Exception:
        pass

    try:
        cursor = conn.execute("PRAGMA table_info(comment_verification_runs)")
        columns = [info[1] for info in cursor.fetchall()]
        verification_columns = {
            "comment_job_id": "ALTER TABLE comment_verification_runs ADD COLUMN comment_job_id INTEGER",
            "verifier_account_id": "ALTER TABLE comment_verification_runs ADD COLUMN verifier_account_id TEXT",
            "verifier_account_name": "ALTER TABLE comment_verification_runs ADD COLUMN verifier_account_name TEXT",
            "status": "ALTER TABLE comment_verification_runs ADD COLUMN status TEXT DEFAULT 'pending'",
            "verdict": "ALTER TABLE comment_verification_runs ADD COLUMN verdict TEXT",
            "matched_comment_id": "ALTER TABLE comment_verification_runs ADD COLUMN matched_comment_id TEXT",
            "matched_comment_content": "ALTER TABLE comment_verification_runs ADD COLUMN matched_comment_content TEXT",
            "matched_comment_like_count": "ALTER TABLE comment_verification_runs ADD COLUMN matched_comment_like_count INTEGER DEFAULT 0",
            "matched_comment_create_time": "ALTER TABLE comment_verification_runs ADD COLUMN matched_comment_create_time TEXT",
            "matched_by": "ALTER TABLE comment_verification_runs ADD COLUMN matched_by TEXT",
            "reason": "ALTER TABLE comment_verification_runs ADD COLUMN reason TEXT",
            "error_message": "ALTER TABLE comment_verification_runs ADD COLUMN error_message TEXT",
            "result_json": "ALTER TABLE comment_verification_runs ADD COLUMN result_json TEXT",
            "verified_at": "ALTER TABLE comment_verification_runs ADD COLUMN verified_at TEXT",
            "updated_at": "ALTER TABLE comment_verification_runs ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
        for column_name, statement in verification_columns.items():
            if column_name not in columns:
                conn.execute(statement)
    except Exception:
        pass

    try:
        cursor = conn.execute("PRAGMA table_info(verifier_accounts)")
        columns = [info[1] for info in cursor.fetchall()]
        verifier_columns = {
            "account_name": "ALTER TABLE verifier_accounts ADD COLUMN account_name TEXT",
            "status": "ALTER TABLE verifier_accounts ADD COLUMN status TEXT DEFAULT 'active'",
            "is_default": "ALTER TABLE verifier_accounts ADD COLUMN is_default INTEGER DEFAULT 0",
            "notes": "ALTER TABLE verifier_accounts ADD COLUMN notes TEXT",
            "updated_at": "ALTER TABLE verifier_accounts ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
        for column_name, statement in verifier_columns.items():
            if column_name not in columns:
                conn.execute(statement)
    except Exception:
        pass

    try:
        cursor = conn.execute("PRAGMA table_info(daily_boss_reports)")
        columns = [info[1] for info in cursor.fetchall()]
        if "summary_json" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN summary_json TEXT")
        if "employee_count" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN employee_count INTEGER DEFAULT 0")
        if "total_target_crawl_count" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN total_target_crawl_count INTEGER DEFAULT 0")
        if "total_target_comment_count" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN total_target_comment_count INTEGER DEFAULT 0")
        if "total_actual_note_count" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN total_actual_note_count INTEGER DEFAULT 0")
        if "total_sent_comment_count" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN total_sent_comment_count INTEGER DEFAULT 0")
        if "total_failed_comment_count" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN total_failed_comment_count INTEGER DEFAULT 0")
        if "llm_model" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN llm_model TEXT")
        if "prompt_tokens" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN prompt_tokens INTEGER DEFAULT 0")
        if "completion_tokens" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN completion_tokens INTEGER DEFAULT 0")
        if "total_tokens" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN total_tokens INTEGER DEFAULT 0")
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE daily_boss_reports ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass

    return conn


@st.cache_resource(show_spinner=False)
def ensure_dashboard_storage_ready():
    """Initialize storage on first boot so cloud deployments can start from an empty disk."""
    os.makedirs(DATABASE_DIR, exist_ok=True)
    os.makedirs(CONFIG_DIR, exist_ok=True)

    from database.db import init_db as init_orm_db

    try:
        asyncio.run(init_orm_db("sqlite"))
    except RuntimeError as exc:
        if "asyncio.run() cannot be called from a running event loop" not in str(exc):
            raise
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(init_orm_db("sqlite"))
        finally:
            loop.close()

    conn = get_tasks_connection()
    conn.close()
    return {
        "db_path": DB_PATH,
        "tasks_db_path": TASKS_DB_PATH,
    }


try:
    ensure_dashboard_storage_ready()
except Exception as exc:
    st.error("系统初始化失败，暂时无法加载工作台。")
    st.exception(exc)
    st.stop()

def delete_task(task_id):
    conn = get_tasks_connection()
    conn.execute("DELETE FROM crawler_tasks WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()

def update_note_status(note_id, status):
    conn = get_connection()
    conn.execute("UPDATE xhs_note SET comment_status = ? WHERE note_id = ?", (status, note_id))
    conn.commit()
    conn.close()

def delete_note(note_id):
    conn = get_connection()
    conn.execute("DELETE FROM xhs_note WHERE note_id = ?", (note_id,))
    conn.execute("DELETE FROM xhs_note_comment WHERE note_id = ?", (note_id,))
    conn.commit()
    conn.close()

def format_rule(sort_type, max_count, start_date, end_date, filter_condition=""):
    normalized_filter_condition = str(filter_condition or "").strip()
    if normalized_filter_condition.lower() == "nan":
        normalized_filter_condition = ""

    rule_str = ""
    if sort_type == "综合排序":
        rule_str += "综合排序"
    elif sort_type == "最热":
        rule_str += "按点赞量"
    elif sort_type == "最新":
        rule_str += "最新发布"
        
    if start_date or end_date:
        if start_date and end_date:
            rule_str += f" ({start_date} ~ {end_date})"
        elif start_date:
            rule_str += f" ({start_date}之后)"
        elif end_date:
            rule_str += f" ({end_date}之前)"
            
    rule_str += f" 前{max_count}个"
    if normalized_filter_condition:
        rule_str += " + AI粗筛"
    return rule_str

def get_account_stats():
    conn = get_connection()
    try:
        # Total count per account
        total_query = """
        SELECT 
            account_name, 
            COUNT(*) as total_count 
        FROM xhs_note 
        WHERE account_name IS NOT NULL AND account_name != '' AND dashboard_user = ?
        GROUP BY account_name
        """
        total_df = pd.read_sql_query(total_query, conn, params=(st.session_state.username,))
        
        # Daily count per account (last 7 days)
        daily_query = """
        SELECT 
            account_name,
            date(last_modify_ts / 1000, 'unixepoch', 'localtime') as crawl_date,
            COUNT(*) as daily_count
        FROM xhs_note
        WHERE account_name IS NOT NULL AND account_name != '' AND dashboard_user = ?
        GROUP BY account_name, crawl_date
        ORDER BY crawl_date DESC
        """
        daily_df = pd.read_sql_query(daily_query, conn, params=(st.session_state.username,))
    except Exception as e:
        total_df = pd.DataFrame()
        daily_df = pd.DataFrame()
    finally:
        conn.close()
        
    return total_df, daily_df

def load_accounts():
    if not os.path.exists(ACCOUNTS_FILE):
        return []
    try:
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def save_accounts(accounts):
    os.makedirs(os.path.dirname(ACCOUNTS_FILE), exist_ok=True)
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, ensure_ascii=False, indent=2)

def load_comment_executor_accounts(username):
    os.makedirs(os.path.dirname(COMMENT_EXECUTOR_ACCOUNTS_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_EXECUTOR_ACCOUNTS_FILE):
            with open(COMMENT_EXECUTOR_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                all_accounts = json.load(f)
            if not isinstance(all_accounts, list):
                all_accounts = []
        else:
            all_accounts = []
    except Exception:
        all_accounts = []
    return [
        acc for acc in all_accounts
        if str(acc.get("dashboard_user", "")) == str(username)
    ]

def save_comment_executor_accounts_for_user(username, user_accounts):
    os.makedirs(os.path.dirname(COMMENT_EXECUTOR_ACCOUNTS_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_EXECUTOR_ACCOUNTS_FILE):
            with open(COMMENT_EXECUTOR_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                all_accounts = json.load(f)
            if not isinstance(all_accounts, list):
                all_accounts = []
        else:
            all_accounts = []
    except Exception:
        all_accounts = []

    kept = [
        a for a in all_accounts
        if str(a.get("dashboard_user", "")) != str(username)
    ]
    for acc in user_accounts:
        copied = dict(acc)
        copied["dashboard_user"] = str(username)
        kept.append(copied)

    with open(COMMENT_EXECUTOR_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def load_llm_runtime_settings_for_user(username):
    os.makedirs(os.path.dirname(LLM_RUNTIME_SETTINGS_FILE), exist_ok=True)
    try:
        if os.path.exists(LLM_RUNTIME_SETTINGS_FILE):
            with open(LLM_RUNTIME_SETTINGS_FILE, "r", encoding="utf-8") as f:
                all_settings = json.load(f)
            if not isinstance(all_settings, list):
                all_settings = []
        else:
            all_settings = []
    except Exception:
        all_settings = []
    matched = next(
        (
            item for item in all_settings
            if str(item.get("dashboard_user", "")) == str(username)
        ),
        {},
    )
    return {
        "dashboard_user": str(username),
        "api_key": str(matched.get("api_key", "")).strip(),
        "base_url": _normalize_openai_base_url(matched.get("base_url", "")),
        "model": str(matched.get("model", "")).strip(),
        "updated_at": str(matched.get("updated_at", "")),
    }

def save_llm_runtime_settings_for_user(username, settings):
    os.makedirs(os.path.dirname(LLM_RUNTIME_SETTINGS_FILE), exist_ok=True)
    try:
        if os.path.exists(LLM_RUNTIME_SETTINGS_FILE):
            with open(LLM_RUNTIME_SETTINGS_FILE, "r", encoding="utf-8") as f:
                all_settings = json.load(f)
            if not isinstance(all_settings, list):
                all_settings = []
        else:
            all_settings = []
    except Exception:
        all_settings = []

    matched = next(
        (
            item for item in all_settings
            if str(item.get("dashboard_user", "")) == str(username)
        ),
        {},
    )
    kept = [
        item for item in all_settings
        if str(item.get("dashboard_user", "")) != str(username)
    ]
    merged_settings = {
        "api_key": str((settings or {}).get("api_key", matched.get("api_key", ""))).strip(),
        "base_url": _normalize_openai_base_url((settings or {}).get("base_url", matched.get("base_url", ""))),
        "model": str((settings or {}).get("model", matched.get("model", ""))).strip(),
    }
    kept.append(
        {
            "dashboard_user": str(username),
            "api_key": merged_settings["api_key"],
            "base_url": merged_settings["base_url"],
            "model": merged_settings["model"],
            "updated_at": _now_str(),
        }
    )
    with open(LLM_RUNTIME_SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def _normalize_openai_base_url(base_url):
    raw = str(base_url or "").strip()
    if not raw:
        return ""
    candidate = raw if "://" in raw else f"https://{raw}"
    parsed = urllib.parse.urlparse(candidate)
    if not parsed.netloc:
        return ""
    normalized = f"{parsed.scheme or 'https'}://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")

def _mask_secret(secret, left=6, right=4):
    value = str(secret or "").strip()
    if not value:
        return ""
    if len(value) <= left + right:
        return "*" * len(value)
    return f"{value[:left]}{'*' * (len(value) - left - right)}{value[-right:]}"


def _mcp_port_is_open(host=MCP_HOST, port=MCP_PORT, timeout=0.8):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, int(port))) == 0


def _resolve_mcp_binary_path():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    candidates = [
        os.path.join(base_dir, "xiaohongshu-mcp-darwin-arm64"),
        os.path.join(base_dir, "xiaohongshu-mcp", "xiaohongshu-mcp-darwin-arm64"),
    ]
    for path in candidates:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    for path in candidates:
        if os.path.isfile(path):
            raise RuntimeError(f"已找到评论服务二进制，但没有执行权限：{path}")
    raise RuntimeError(
        "未找到评论服务二进制文件 xiaohongshu-mcp-darwin-arm64。"
        "请确认文件存在于项目根目录或 xiaohongshu-mcp 子目录。"
    )


def _resolve_mcp_browser_binary_path():
    for path in MCP_BROWSER_CANDIDATES:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return ""


def _find_pid_listening_mcp_port():
    try:
        output = subprocess.check_output(
            ["lsof", "-nP", "-t", f"-iTCP:{MCP_PORT}", "-sTCP:LISTEN"],
            text=True,
            timeout=2,
        ).strip()
    except Exception:
        return None
    if not output:
        return None
    first_line = output.splitlines()[0].strip()
    if not first_line.isdigit():
        return None
    return int(first_line)


def _stop_mcp_process():
    pid = _find_pid_listening_mcp_port()
    if not pid:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        return
    deadline = time.time() + 3
    while time.time() < deadline:
        if not _mcp_port_is_open():
            return
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except Exception:
        pass


def _wait_mcp_health(timeout_seconds=MCP_STARTUP_TIMEOUT_SECONDS):
    deadline = time.time() + max(1, int(timeout_seconds))
    last_error = ""
    while time.time() < deadline:
        if not _mcp_port_is_open():
            time.sleep(0.4)
            continue
        try:
            request = urllib.request.Request(f"{MCP_BASE_URL}/health", method="GET")
            with urllib.request.urlopen(request, timeout=2) as response:
                raw = response.read().decode("utf-8", errors="ignore")
            if not raw.strip():
                return True
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and parsed.get("success") is False:
                last_error = str(parsed.get("error") or parsed.get("message") or "健康检查返回失败")
            else:
                return True
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.4)
    if last_error:
        raise RuntimeError(f"评论服务启动后健康检查未通过：{last_error}")
    raise RuntimeError("评论服务启动超时：18060 端口在等待期内未就绪。")


def _ensure_mcp_running():
    if _mcp_port_is_open():
        return
    with MCP_AUTOSTART_LOCK:
        if _mcp_port_is_open():
            return
        binary_path = _resolve_mcp_binary_path()
        os.makedirs(MCP_AUTOSTART_COOKIE_STORE_DIR, exist_ok=True)
        env = os.environ.copy()
        env["COOKIES_PATH"] = MCP_AUTOSTART_COOKIE_PATH
        env["COOKIES_STORE_DIR"] = MCP_AUTOSTART_COOKIE_STORE_DIR
        browser_bin = _resolve_mcp_browser_binary_path()
        if browser_bin:
            env["ROD_BROWSER_BIN"] = browser_bin
        try:
            subprocess.Popen(
                # 非无头模式在当前环境下更稳定，避免 Chromium 无头崩溃导致二维码接口失败。
                [binary_path, "-headless=false", "-port", f":{MCP_PORT}"],
                cwd=os.path.dirname(binary_path),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except PermissionError as exc:
            raise RuntimeError(f"评论服务二进制无法执行：{binary_path}") from exc
        except Exception as exc:
            raise RuntimeError(f"自动启动评论服务失败：{exc}") from exc
        _wait_mcp_health()


def _perform_http_request(url, method="GET", payload=None, timeout=60):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def _parse_error_payload(exc):
    try:
        raw = exc.read().decode("utf-8", errors="ignore")
    except Exception:
        raw = ""
    if not raw.strip():
        return raw, {}
    try:
        parsed = json.loads(raw)
        return raw, parsed if isinstance(parsed, dict) else {}
    except Exception:
        return raw, {}


def _format_mcp_http_error(exc):
    raw, parsed = _parse_error_payload(exc)
    if str(exc.url or "").startswith(MCP_BASE_URL):
        error_code = str(parsed.get("code") or "").strip()
        error_text = str(parsed.get("error") or "").strip()
        if error_code == "INTERNAL_ERROR" and error_text == "服务器内部错误":
            return (
                "评论服务内部浏览器异常退出（Chromium crash）。"
                "已优先切换系统 Chrome 自动拉起，请重试一次；"
                "若仍失败，请先关闭系统弹出的 Chromium 崩溃对话框后再试。"
            )
    return f"评论服务请求失败（HTTP {exc.code}）：{raw[:500] or exc.reason}"


def _http_json_request(url, method="GET", payload=None, timeout=60, _allow_restart=True):
    try:
        raw = _perform_http_request(url, method=method, payload=payload, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if str(url).startswith(MCP_BASE_URL) and _allow_restart:
            raw_body, parsed = _parse_error_payload(exc)
            if str(parsed.get("code") or "").strip() == "INTERNAL_ERROR":
                _stop_mcp_process()
                _ensure_mcp_running()
                return _http_json_request(
                    url,
                    method=method,
                    payload=payload,
                    timeout=timeout,
                    _allow_restart=False,
                )
            raise RuntimeError(
                f"评论服务请求失败（HTTP {exc.code}）：{raw_body[:500] or exc.reason}"
            ) from exc
        raise RuntimeError(_format_mcp_http_error(exc)) from exc
    except urllib.error.URLError as exc:
        if str(url).startswith(MCP_BASE_URL):
            try:
                _ensure_mcp_running()
                raw = _perform_http_request(url, method=method, payload=payload, timeout=timeout)
            except urllib.error.HTTPError as retry_http_exc:
                body = retry_http_exc.read().decode("utf-8", errors="ignore")
                raise RuntimeError(
                    f"评论服务请求失败（HTTP {retry_http_exc.code}）：{body[:500] or retry_http_exc.reason}"
                ) from retry_http_exc
            except Exception as start_exc:
                raise RuntimeError(
                    "无法连接评论服务，且自动拉起失败："
                    f"{start_exc}"
                ) from start_exc
        else:
            raise RuntimeError("网络请求失败，请稍后重试。") from exc
    except OSError as exc:
        if str(url).startswith(MCP_BASE_URL):
            try:
                _ensure_mcp_running()
                raw = _perform_http_request(url, method=method, payload=payload, timeout=timeout)
            except Exception as start_exc:
                raise RuntimeError(
                    "无法连接评论服务，且自动拉起失败："
                    f"{start_exc}"
                ) from start_exc
        else:
            raise RuntimeError("网络请求失败，请稍后重试。") from exc

    if not isinstance(raw, str):
        raise RuntimeError(
            "评论服务返回了非法响应。"
        )

    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except Exception as exc:
        raise RuntimeError("评论服务返回了无法解析的 JSON 数据。") from exc

def _normalize_mcp_account_id(account_id):
    normalized = str(account_id or "").strip()
    if normalized.lower() in {"", "default", MCP_DEFAULT_RUNTIME_ACCOUNT_ID.lower()}:
        return ""
    return normalized


def _build_mcp_url(path, query_params=None):
    normalized_params = {
        str(k): str(v)
        for k, v in (query_params or {}).items()
        if str(v or "").strip()
    }
    if not normalized_params:
        return f"{MCP_BASE_URL}{path}"
    return f"{MCP_BASE_URL}{path}?{urllib.parse.urlencode(normalized_params)}"


def _check_mcp_comment_api_status(account_id=""):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    health_resp = _http_json_request(f"{MCP_BASE_URL}/health", method="GET", timeout=5)
    if isinstance(health_resp, dict) and health_resp.get("success") is False:
        raise RuntimeError(health_resp.get("error") or "评论服务健康检查失败。")

    login_status_url = _build_mcp_url("/api/v1/login/status", {"account_id": normalized_account_id})
    login_resp = _http_json_request(login_status_url, method="GET", timeout=10)
    if not isinstance(login_resp, dict):
        raise RuntimeError("评论服务登录状态检查失败。")
    if login_resp.get("success") is False:
        raise RuntimeError(
            login_resp.get("error") or login_resp.get("message") or "评论服务登录状态检查失败。"
        )

    login_data = login_resp.get("data") or {}
    if not isinstance(login_data, dict):
        raise RuntimeError("评论服务返回的登录状态格式不正确。")
    if not login_data.get("is_logged_in"):
        # 扫码登录态写入是异步的，给短暂缓冲窗口，避免用户“扫码后立刻检查”被误判。
        for _ in range(8):
            time.sleep(1)
            retry_resp = _http_json_request(login_status_url, method="GET", timeout=10)
            retry_data = retry_resp.get("data") if isinstance(retry_resp, dict) else {}
            if isinstance(retry_data, dict) and retry_data.get("is_logged_in"):
                login_data = retry_data
                break
        if not login_data.get("is_logged_in"):
            account_hint = normalized_account_id or "默认运行时账号"
            raise RuntimeError(
                f"评论服务账号[{account_hint}]尚未登录小红书。"
                "请在手机端确认登录，并确保系统打开的浏览器窗口未被关闭；"
                "如仍失败，请重新获取二维码后再扫码。"
            )
    if normalized_account_id and not login_data.get("account_id"):
        login_data["account_id"] = normalized_account_id
    return login_data


def _get_mcp_login_qrcode(account_id=""):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    resp = _http_json_request(
        _build_mcp_url("/api/v1/login/qrcode", {"account_id": normalized_account_id}),
        method="GET",
        timeout=30,
    )
    if not isinstance(resp, dict):
        raise RuntimeError("评论服务返回的扫码数据格式不正确。")
    if resp.get("success") is False:
        raise RuntimeError(resp.get("error") or resp.get("message") or "获取评论服务登录二维码失败。")
    data = resp.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("评论服务返回的扫码数据格式不正确。")
    return data


def _delete_mcp_login_cookies(account_id=""):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    resp = _http_json_request(
        _build_mcp_url("/api/v1/login/cookies", {"account_id": normalized_account_id}),
        method="DELETE",
        timeout=15,
    )
    if not isinstance(resp, dict):
        raise RuntimeError("评论服务返回的清理登录态结果格式不正确。")
    if resp.get("success") is False:
        raise RuntimeError(resp.get("error") or resp.get("message") or "清理评论服务登录态失败。")
    data = resp.get("data") or {}
    return data if isinstance(data, dict) else {}


def _get_mcp_my_profile(account_id=""):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    resp = _http_json_request(
        _build_mcp_url("/api/v1/user/me", {"account_id": normalized_account_id}),
        method="GET",
        timeout=30,
    )
    if not isinstance(resp, dict):
        raise RuntimeError("评论服务返回的个人主页数据格式不正确。")
    if resp.get("success") is False:
        raise RuntimeError(resp.get("error") or resp.get("message") or "获取评论服务个人主页失败。")
    outer_data = resp.get("data") or {}
    if not isinstance(outer_data, dict):
        raise RuntimeError("评论服务返回的个人主页数据格式不正确。")
    profile_data = outer_data.get("data") or outer_data
    if not isinstance(profile_data, dict):
        raise RuntimeError("评论服务返回的个人主页数据格式不正确。")
    return profile_data


def _fetch_mcp_feed_detail(note_id, xsec_token, account_id="", load_all_comments=True):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    payload = {
        "feed_id": str(note_id or "").strip(),
        "xsec_token": str(xsec_token or "").strip(),
        "load_all_comments": bool(load_all_comments),
        "comment_config": {
            "click_more_replies": True,
            "max_replies_threshold": 0,
            "max_comment_items": 0 if load_all_comments else 20,
            "scroll_speed": "normal",
        },
    }
    if normalized_account_id:
        payload["account_id"] = normalized_account_id
    resp = _http_json_request(
        f"{MCP_BASE_URL}/api/v1/feeds/detail",
        method="POST",
        payload=payload,
        timeout=180 if load_all_comments else 90,
    )
    if not isinstance(resp, dict):
        raise RuntimeError("评论服务返回的笔记详情格式不正确。")
    if resp.get("success") is False:
        raise RuntimeError(resp.get("error") or resp.get("message") or "获取笔记详情失败。")
    outer_data = resp.get("data") or {}
    if not isinstance(outer_data, dict):
        raise RuntimeError("评论服务返回的笔记详情格式不正确。")
    detail_data = outer_data.get("data") or outer_data
    if not isinstance(detail_data, dict):
        raise RuntimeError("评论服务返回的笔记详情格式不正确。")
    return detail_data

def _get_note_xsec_token(note):
    token = str(note.get("xsec_token") or "").strip()
    if token:
        return token

    note_url = str(note.get("note_url") or "").strip()
    if not note_url:
        return ""
    try:
        parsed = urllib.parse.urlparse(note_url)
        return str((urllib.parse.parse_qs(parsed.query).get("xsec_token") or [""])[0]).strip()
    except Exception:
        return ""

def trigger_third_party_comment_api(account_id, payload):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    login_data = _check_mcp_comment_api_status(normalized_account_id)
    target_type = str(payload.get("target_type") or "note").strip() or "note"
    note_id = str(payload.get("note_id") or "").strip()
    xsec_token = str(payload.get("xsec_token") or "").strip()
    comment_content = str(payload.get("comment_content") or "").strip()

    if not note_id:
        raise RuntimeError("缺少笔记 ID，无法发送评论。")
    if not xsec_token:
        raise RuntimeError("当前笔记缺少 xsec_token，无法发送评论。")
    if not comment_content:
        raise RuntimeError("评论内容不能为空。")

    if target_type == "reply":
        comment_id = str(payload.get("target_comment_id") or "").strip()
        user_id = str(payload.get("target_user_id") or "").strip()
        if not comment_id and not user_id:
            raise RuntimeError("回复评论时必须提供 comment_id 或 user_id。")
        request_payload = {
            "feed_id": note_id,
            "xsec_token": xsec_token,
            "content": comment_content,
        }
        if normalized_account_id:
            request_payload["account_id"] = normalized_account_id
        if comment_id:
            request_payload["comment_id"] = comment_id
        if user_id:
            request_payload["user_id"] = user_id
        response = _http_json_request(
            f"{MCP_BASE_URL}/api/v1/feeds/comment/reply",
            method="POST",
            payload=request_payload,
            timeout=120,
        )
    else:
        request_payload = {
            "feed_id": note_id,
            "xsec_token": xsec_token,
            "content": comment_content,
        }
        if normalized_account_id:
            request_payload["account_id"] = normalized_account_id
        response = _http_json_request(
            f"{MCP_BASE_URL}/api/v1/feeds/comment",
            method="POST",
            payload=request_payload,
            timeout=120,
        )

    if not isinstance(response, dict) or response.get("success") is False:
        raise RuntimeError(
            (response.get("error") or response.get("message") or "评论服务返回失败。")
            if isinstance(response, dict)
            else "评论服务返回失败。"
        )

    response_data = response.get("data") or {}
    return {
        "ok": True,
        "account_id": normalized_account_id or MCP_DEFAULT_RUNTIME_ACCOUNT_ID,
        "request_id": f"mcp_{int(time.time() * 1000)}",
        "payload": request_payload,
        "response": response_data,
        "message": response.get("message") or response_data.get("message") or "发送成功",
        "mcp_username": str(
            response_data.get("account_id")
            or login_data.get("account_id")
            or normalized_account_id
            or MCP_DEFAULT_RUNTIME_ACCOUNT_ID
            or login_data.get("username")
        ),
        "comment_id": str(response_data.get("comment_id") or ""),
        "target_type": target_type,
    }

def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@st.fragment(run_every="5s")
def render_task_list_fragment():
    st.header("任务列表")
    reconcile_running_tasks()
    reconcile_completed_tasks()
    tasks_df = get_tasks()
    if not tasks_df.empty:
        active_task_count = int(tasks_df["status"].isin(["Running", "Pending"]).sum())
        if active_task_count > 0:
            st.caption(f"检测到 {active_task_count} 个进行中/等待中的任务，列表每 5 秒局部自动刷新。")

        # Table Header
        h1, h2, h3, h4, h5, h6 = st.columns([2, 3, 1, 2, 1, 2])
        h1.markdown("**关键词**")
        h2.markdown("**爬取规则**")
        h3.markdown("**已爬取**")
        h4.markdown("**创建时间**")
        h5.markdown("**状态**")
        h6.markdown("**操作**")
        st.divider()

        for _, task in tasks_df.iterrows():
            with st.container():
                c1, c2, c3, c4, c5, c6 = st.columns([2, 3, 1, 2, 1, 2])
                c1.write(task['keyword'])

                # Format Rule
                rule_desc = format_rule(
                    task['sort_type'],
                    task['max_count'],
                    task['start_date'] if task['start_date'] != 'None' else None,
                    task['end_date'] if task['end_date'] != 'None' else None,
                    task.get('filter_condition', ''),
                )
                c2.write(rule_desc)
                filter_condition_text = str(task.get('filter_condition', '') or '').strip()
                if filter_condition_text and filter_condition_text.lower() != "nan":
                    preview = filter_condition_text[:30] + ("..." if len(filter_condition_text) > 30 else "")
                    c2.caption(f"过滤条件：{preview}")
                if parse_bool_flag(task.get("low_risk_mode", 0), default=False):
                    c2.caption("低风险模式：已开启")

                # We don't have real-time crawled count in tasks table yet, using max_count as placeholder or need a join query.
                crawled_count = get_task_crawled_count(task)
                max_count_display = int(task['max_count']) if str(task.get('max_count', '')).strip() else 0
                safe_crawled_count = min(crawled_count, max_count_display) if max_count_display > 0 else crawled_count
                c3.write(f"{safe_crawled_count} / {max_count_display} 条")

                c4.write(task['created_at'])

                # Status with style
                status = task['status']
                if status == "Running":
                    c5.markdown(":green[进行中]")
                elif status == "Pending":
                    c5.markdown(":grey[等待中]")
                elif status == "Completed":
                    c5.markdown(":blue[已完成]")
                elif status == "Stopped":
                    c5.markdown(":orange[已暂停]")
                elif status == "Error":
                    c5.markdown(":red[失败]")
                else:
                    c5.write(status)
                if task.get('last_error'):
                    err = str(task['last_error'])
                    c5.caption(err[:120] + ("..." if len(err) > 120 else ""))
                    guidance = get_task_error_guidance(err)
                    if guidance:
                        c5.caption(guidance)

                # Actions
                with c6:
                    ac1, ac2, ac3 = st.columns([1, 1, 2])
                    # Pause/Resume
                    if status == "Running":
                        if ac1.button("⏸️", key=f"stop_{task['id']}", help="暂停"):
                            normalized_pid = normalize_task_pid(task.get('pid'))
                            if normalized_pid is not None:
                                try:
                                    os.kill(normalized_pid, signal.SIGTERM)
                                    update_task_status(task['id'], "Stopped")
                                except ProcessLookupError:
                                    update_task_status(task['id'], "Error", error_message="任务进程不存在，可能已异常退出。")
                                except Exception as e:
                                    update_task_status(task['id'], "Error", error_message=f"暂停失败：{e}")
                            else:
                                update_task_status(task['id'], "Error", error_message="暂停失败：任务PID无效，可能已退出。")
                            st.rerun()
                    else:
                        if ac1.button("▶️", key=f"run_{task['id']}", help="启动"):
                            # Handle missing account_name for old records
                            acc_name = task.get('account_name')
                            available, reason = has_available_login_state(acc_name)
                            if not available:
                                st.error(reason)
                            else:
                                default_scan_warning = get_default_scan_login_warning(acc_name)
                                if default_scan_warning:
                                    st.warning(default_scan_warning)
                                ok, preflight_error = preflight_browser_check()
                                if not ok:
                                    is_preflight_timeout = "浏览器自检超时" in str(preflight_error or "")
                                    if not is_preflight_timeout:
                                        st.error(preflight_error or "浏览器自检失败，任务未启动。")
                                    else:
                                        st.warning("浏览器自检超时，已跳过自检并尝试启动任务。")
                                if ok or is_preflight_timeout:
                                    run_crawler(
                                        task['id'],
                                        task['keyword'],
                                        task['sort_type'],
                                        task['max_count'],
                                        acc_name,
                                        task.get('filter_condition', ''),
                                        parse_bool_flag(task.get("low_risk_mode", 0), default=False),
                                    )
                                    st.rerun()

                    # Delete
                    if ac2.button("🗑️", key=f"del_{task['id']}", help="删除"):
                        delete_task(task['id'])
                        st.rerun()

                    # View Results
                    if ac3.button("查看结果", key=f"view_{task['id']}"):
                        st.session_state.view_keyword = task['keyword']
                        st.session_state.view_source_keyword = task['keyword']
                        st.session_state.note_filters_reset_requested = True
                        switch_dashboard_page("笔记展示")

                st.divider()
    else:
        st.info("暂无任务")

def _default_comment_templates(username):
    now = _now_str()
    return [
        {
            "id": int(time.time() * 1000),
            "dashboard_user": username,
            "name": "私聊引导模板",
            "category": "截流型",
            "scene": "穿搭引流",
            "content": "姐妹这套真的很绝，我这边刚好整理了同款对比图，要不要我发你参考下？",
            "emoji": "✨👗",
            "image_url": "",
            "tags": ["私聊", "引流", "穿搭"],
            "status": "active",
            "used_count": 0,
            "created_at": now,
            "updated_at": now,
            "last_used_at": "-",
        },
        {
            "id": int(time.time() * 1000) + 1,
            "dashboard_user": username,
            "name": "好物种草模板",
            "category": "种草型",
            "scene": "美妆种草",
            "content": "这个色号我自己回购过，黄皮也友好，质地很轻薄不拔干，真心推荐你试试看～",
            "emoji": "🌟💄",
            "image_url": "",
            "tags": ["种草", "显白"],
            "status": "active",
            "used_count": 0,
            "created_at": now,
            "updated_at": now,
            "last_used_at": "-",
        },
    ]

def load_comment_templates(username):
    os.makedirs(os.path.dirname(COMMENT_TEMPLATES_FILE), exist_ok=True)
    if not os.path.exists(COMMENT_TEMPLATES_FILE):
        defaults = _default_comment_templates(username)
        save_comment_templates_for_user(username, defaults)
        return defaults
    try:
        with open(COMMENT_TEMPLATES_FILE, "r", encoding="utf-8") as f:
            all_templates = json.load(f)
        if not isinstance(all_templates, list):
            all_templates = []
    except Exception:
        all_templates = []

    user_templates = [
        t for t in all_templates
        if str(t.get("dashboard_user", "")) == str(username)
    ]
    if not user_templates:
        user_templates = _default_comment_templates(username)
        save_comment_templates_for_user(username, user_templates)
    return user_templates

def save_comment_templates_for_user(username, user_templates):
    os.makedirs(os.path.dirname(COMMENT_TEMPLATES_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_TEMPLATES_FILE):
            with open(COMMENT_TEMPLATES_FILE, "r", encoding="utf-8") as f:
                all_templates = json.load(f)
            if not isinstance(all_templates, list):
                all_templates = []
        else:
            all_templates = []
    except Exception:
        all_templates = []

    kept = [
        t for t in all_templates
        if str(t.get("dashboard_user", "")) != str(username)
    ]
    for t in user_templates:
        copied = dict(t)
        copied["dashboard_user"] = username
        kept.append(copied)

    with open(COMMENT_TEMPLATES_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def compose_template_preview(template):
    if not template:
        return ""
    emoji = f" {template.get('emoji', '').strip()}" if template.get("emoji") else ""
    image_hint = " [含图片]" if template.get("image_url") else ""
    return f"{template.get('content', '')}{emoji}{image_hint}".strip()

def _default_comment_strategy():
    return {
        "daily_limit": 120,
        "hourly_limit": 20,
        "jitter_min_sec": 45,
        "jitter_max_sec": 180,
        "active_start": "08:30",
        "active_end": "23:00",
        "updated_at": _now_str(),
    }

def _strategy_from_package(package):
    return {
        "daily_limit": int(package.get("daily_limit", 120)),
        "hourly_limit": int(package.get("hourly_limit", 20)),
        "jitter_min_sec": int(package.get("jitter_min_sec", 45)),
        "jitter_max_sec": int(package.get("jitter_max_sec", 180)),
        "active_start": str(package.get("active_start", "08:30")),
        "active_end": str(package.get("active_end", "23:00")),
    }

def _default_comment_strategy_package(username):
    base = _default_comment_strategy()
    return {
        "id": f"pkg_{int(time.time() * 1000)}",
        "dashboard_user": username,
        "name": "默认策略包",
        "description": "默认风控策略",
        "daily_limit": int(base["daily_limit"]),
        "hourly_limit": int(base["hourly_limit"]),
        "jitter_min_sec": int(base["jitter_min_sec"]),
        "jitter_max_sec": int(base["jitter_max_sec"]),
        "active_start": str(base["active_start"]),
        "active_end": str(base["active_end"]),
        "updated_at": _now_str(),
    }

def load_comment_strategy_packages(username):
    os.makedirs(os.path.dirname(COMMENT_STRATEGY_PACKAGES_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_STRATEGY_PACKAGES_FILE):
            with open(COMMENT_STRATEGY_PACKAGES_FILE, "r", encoding="utf-8") as f:
                all_packages = json.load(f)
            if not isinstance(all_packages, list):
                all_packages = []
        else:
            all_packages = []
    except Exception:
        all_packages = []

    user_packages = [
        p for p in all_packages
        if str(p.get("dashboard_user", "")) == str(username)
    ]
    if not user_packages:
        base = load_comment_strategy(username)
        default_pkg = _default_comment_strategy_package(username)
        default_pkg.update({
            "daily_limit": int(base.get("daily_limit", default_pkg["daily_limit"])),
            "hourly_limit": int(base.get("hourly_limit", default_pkg["hourly_limit"])),
            "jitter_min_sec": int(base.get("jitter_min_sec", default_pkg["jitter_min_sec"])),
            "jitter_max_sec": int(base.get("jitter_max_sec", default_pkg["jitter_max_sec"])),
            "active_start": str(base.get("active_start", default_pkg["active_start"])),
            "active_end": str(base.get("active_end", default_pkg["active_end"])),
            "updated_at": _now_str(),
        })
        user_packages = [default_pkg]
        save_comment_strategy_packages_for_user(username, user_packages)
    return user_packages

def save_comment_strategy_packages_for_user(username, user_packages):
    os.makedirs(os.path.dirname(COMMENT_STRATEGY_PACKAGES_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_STRATEGY_PACKAGES_FILE):
            with open(COMMENT_STRATEGY_PACKAGES_FILE, "r", encoding="utf-8") as f:
                all_packages = json.load(f)
            if not isinstance(all_packages, list):
                all_packages = []
        else:
            all_packages = []
    except Exception:
        all_packages = []

    kept = [
        p for p in all_packages
        if str(p.get("dashboard_user", "")) != str(username)
    ]
    for pkg in user_packages:
        copied = dict(pkg)
        copied["dashboard_user"] = str(username)
        copied["updated_at"] = _now_str()
        kept.append(copied)

    with open(COMMENT_STRATEGY_PACKAGES_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def load_comment_strategy(username):
    os.makedirs(os.path.dirname(COMMENT_STRATEGY_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_STRATEGY_FILE):
            with open(COMMENT_STRATEGY_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                raw = {}
        else:
            raw = {}
    except Exception:
        raw = {}

    strategy = raw.get(str(username))
    if not isinstance(strategy, dict):
        strategy = _default_comment_strategy()
        save_comment_strategy(username, strategy)
    return strategy

def save_comment_strategy(username, strategy):
    os.makedirs(os.path.dirname(COMMENT_STRATEGY_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_STRATEGY_FILE):
            with open(COMMENT_STRATEGY_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                raw = {}
        else:
            raw = {}
    except Exception:
        raw = {}
    copied = dict(strategy)
    copied["updated_at"] = _now_str()
    raw[str(username)] = copied
    with open(COMMENT_STRATEGY_FILE, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

def load_comment_strategy_events(username):
    os.makedirs(os.path.dirname(COMMENT_STRATEGY_EVENT_FILE), exist_ok=True)
    if not os.path.exists(COMMENT_STRATEGY_EVENT_FILE):
        return []
    try:
        with open(COMMENT_STRATEGY_EVENT_FILE, "r", encoding="utf-8") as f:
            all_events = json.load(f)
        if not isinstance(all_events, list):
            return []
    except Exception:
        return []
    return [e for e in all_events if str(e.get("dashboard_user", "")) == str(username)]

def save_comment_strategy_events_for_user(username, events):
    os.makedirs(os.path.dirname(COMMENT_STRATEGY_EVENT_FILE), exist_ok=True)
    try:
        if os.path.exists(COMMENT_STRATEGY_EVENT_FILE):
            with open(COMMENT_STRATEGY_EVENT_FILE, "r", encoding="utf-8") as f:
                all_events = json.load(f)
            if not isinstance(all_events, list):
                all_events = []
        else:
            all_events = []
    except Exception:
        all_events = []
    kept = [e for e in all_events if str(e.get("dashboard_user", "")) != str(username)]
    for e in events:
        copied = dict(e)
        copied["dashboard_user"] = str(username)
        kept.append(copied)
    with open(COMMENT_STRATEGY_EVENT_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def _normalize_exception_source(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"crawl", "comment_send", "comment_verify"}:
        return normalized
    return "crawl"

def _normalize_exception_severity(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"critical", "warn", "info"}:
        return normalized
    return "warn"

def _normalize_exception_status(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"open", "resolved", "ignored"}:
        return normalized
    return "open"

def _operation_exception_source_label(value):
    return OPERATION_EXCEPTION_SOURCE_LABELS.get(str(value or ""), str(value or "-"))

def _operation_exception_severity_label(value):
    return OPERATION_EXCEPTION_SEVERITY_LABELS.get(str(value or ""), str(value or "-"))

def _operation_exception_status_label(value):
    return OPERATION_EXCEPTION_STATUS_LABELS.get(str(value or ""), str(value or "-"))

def _resolve_exception_employee(username, source_module="", employee_id="", crawl_account_name="", comment_account_id="", strategy_package_id=""):
    profiles = load_employee_profiles(username)
    if employee_id:
        matched = next(
            (item for item in profiles if str(item.get("id", "")).strip() == str(employee_id).strip()),
            None,
        )
        if matched:
            return str(matched.get("id", "")).strip(), str(matched.get("name", "")).strip()

    normalized_source = _normalize_exception_source(source_module)
    if normalized_source == "crawl" and str(crawl_account_name or "").strip():
        crawl_name = str(crawl_account_name).strip()
        matched = next(
            (
                item for item in profiles
                if crawl_name in {
                    str(acc).strip()
                    for acc in item.get("assigned_crawl_accounts", [])
                    if str(acc).strip()
                }
            ),
            None,
        )
        if matched:
            return str(matched.get("id", "")).strip(), str(matched.get("name", "")).strip()

    if normalized_source in {"comment_send", "comment_verify"}:
        normalized_account_id = str(comment_account_id or "").strip()
        normalized_pkg_id = str(strategy_package_id or "").strip()
        matched = next(
            (
                item for item in profiles
                if (
                    normalized_account_id
                    and normalized_account_id in {
                        str(acc).strip()
                        for acc in item.get("assigned_comment_accounts", [])
                        if str(acc).strip()
                    }
                )
                or (
                    normalized_pkg_id
                    and normalized_pkg_id in {
                        str(pkg).strip()
                        for pkg in item.get("assigned_strategy_package_ids", [])
                        if str(pkg).strip()
                    }
                )
            ),
            None,
        )
        if matched:
            return str(matched.get("id", "")).strip(), str(matched.get("name", "")).strip()

    return "", ""

def _build_exception_guidance(source_module, error_message):
    source = _normalize_exception_source(source_module)
    err_text = str(error_message or "").strip()
    if source == "crawl":
        return get_task_error_guidance(err_text) or ""
    lowered = err_text.lower()
    if source == "comment_send":
        if "无法连接评论服务" in err_text or "connection refused" in lowered:
            return "建议：先确认 xiaohongshu-mcp 服务已启动且端口正常，再重试失败任务。"
        if "登录" in err_text or "cookie" in lowered:
            return "建议：到“执行中心 > 账号与登录”检查发送账号登录状态，必要时重新扫码登录。"
        return "建议：先在执行中心重试单条任务，确认链路恢复后再放量。"
    if source == "comment_verify":
        if "未配置可用的验证账号" in err_text:
            return "建议：先在“执行中心 > 账号与登录”新增并登录回溯检查账号，再执行检查。"
        if "无法连接" in err_text or "connection" in lowered:
            return "建议：先检查回溯接口连通性，再对待回溯任务批量补跑。"
        return "建议：先人工复核样本，再优化回溯匹配规则。"
    return ""

def load_operation_exceptions(username):
    os.makedirs(os.path.dirname(OPERATION_EXCEPTIONS_FILE), exist_ok=True)
    if not os.path.exists(OPERATION_EXCEPTIONS_FILE):
        return []
    try:
        with open(OPERATION_EXCEPTIONS_FILE, "r", encoding="utf-8") as f:
            all_items = json.load(f)
        if not isinstance(all_items, list):
            return []
    except Exception:
        return []

    rows = []
    for item in all_items:
        if str(item.get("dashboard_user", "")) != str(username):
            continue
        copied = dict(item)
        copied["source_module"] = _normalize_exception_source(copied.get("source_module"))
        copied["severity"] = _normalize_exception_severity(copied.get("severity"))
        copied["status"] = _normalize_exception_status(copied.get("status"))
        copied["occurred_at"] = str(copied.get("occurred_at") or copied.get("created_at") or _now_str())
        copied["created_at"] = str(copied.get("created_at") or copied["occurred_at"])
        copied["updated_at"] = str(copied.get("updated_at") or copied["created_at"])
        rows.append(copied)
    return sorted(
        rows,
        key=lambda item: str(item.get("occurred_at") or item.get("created_at") or ""),
        reverse=True,
    )

def save_operation_exceptions_for_user(username, events):
    os.makedirs(os.path.dirname(OPERATION_EXCEPTIONS_FILE), exist_ok=True)
    try:
        if os.path.exists(OPERATION_EXCEPTIONS_FILE):
            with open(OPERATION_EXCEPTIONS_FILE, "r", encoding="utf-8") as f:
                all_events = json.load(f)
            if not isinstance(all_events, list):
                all_events = []
        else:
            all_events = []
    except Exception:
        all_events = []

    kept = [item for item in all_events if str(item.get("dashboard_user", "")) != str(username)]
    for item in events:
        if not isinstance(item, dict):
            continue
        copied = dict(item)
        copied["dashboard_user"] = str(username)
        copied["source_module"] = _normalize_exception_source(copied.get("source_module"))
        copied["severity"] = _normalize_exception_severity(copied.get("severity"))
        copied["status"] = _normalize_exception_status(copied.get("status"))
        copied["occurred_at"] = str(copied.get("occurred_at") or copied.get("created_at") or _now_str())
        copied["created_at"] = str(copied.get("created_at") or copied["occurred_at"])
        copied["updated_at"] = str(copied.get("updated_at") or copied["created_at"])
        kept.append(copied)
    with open(OPERATION_EXCEPTIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def append_operation_exception(
    username,
    source_module,
    severity="warn",
    error_message_raw="",
    error_message_normalized="",
    guidance="",
    employee_id="",
    employee_name="",
    task_id=None,
    job_id=None,
    note_id="",
    account_id="",
    strategy_package_id="",
    error_code="",
):
    all_items = load_operation_exceptions(username)
    now = _now_str()
    normalized_raw = str(error_message_raw or "").strip()
    normalized_normalized = str(error_message_normalized or normalized_raw).strip()
    normalized_guidance = str(guidance or _build_exception_guidance(source_module, normalized_raw)).strip()
    normalized_employee_id = str(employee_id or "").strip()
    normalized_employee_name = str(employee_name or "").strip()
    if not normalized_employee_id and not normalized_employee_name:
        resolved_id, resolved_name = _resolve_exception_employee(
            username,
            source_module=source_module,
            crawl_account_name=account_id if _normalize_exception_source(source_module) == "crawl" else "",
            comment_account_id=account_id if _normalize_exception_source(source_module) in {"comment_send", "comment_verify"} else "",
            strategy_package_id=strategy_package_id,
        )
        normalized_employee_id = resolved_id
        normalized_employee_name = resolved_name

    all_items.append(
        {
            "exception_id": f"exc_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            "dashboard_user": str(username),
            "occurred_at": now,
            "source_module": _normalize_exception_source(source_module),
            "severity": _normalize_exception_severity(severity),
            "employee_id": normalized_employee_id,
            "employee_name": normalized_employee_name,
            "task_id": int(task_id) if str(task_id).strip() else None,
            "job_id": int(job_id) if str(job_id).strip() else None,
            "note_id": str(note_id or "").strip(),
            "account_id": str(account_id or "").strip(),
            "strategy_package_id": str(strategy_package_id or "").strip(),
            "error_code": str(error_code or "").strip(),
            "error_message_raw": normalized_raw,
            "error_message_normalized": normalized_normalized,
            "guidance": normalized_guidance,
            "status": "open",
            "owner": "",
            "resolved_at": "",
            "created_at": now,
            "updated_at": now,
        }
    )
    save_operation_exceptions_for_user(username, all_items)

def update_operation_exceptions_status(username, exception_ids, next_status):
    normalized_ids = {str(item).strip() for item in (exception_ids or []) if str(item).strip()}
    if not normalized_ids:
        return 0
    rows = load_operation_exceptions(username)
    changed = 0
    now = _now_str()
    for item in rows:
        if str(item.get("exception_id", "")).strip() not in normalized_ids:
            continue
        current_status = _normalize_exception_status(item.get("status"))
        target_status = _normalize_exception_status(next_status)
        if current_status == target_status:
            continue
        item["status"] = target_status
        item["updated_at"] = now
        item["resolved_at"] = now if target_status == "resolved" else ""
        changed += 1
    if changed > 0:
        save_operation_exceptions_for_user(username, rows)
    return changed

def _employee_type_label(value):
    if str(value) == "seeding":
        return "种草"
    if str(value) == "lead_generation":
        return "引流"
    if str(value) == "strategy_lead":
        return "分析"
    return str(value or "-")

def _employee_status_label(value):
    if str(value) == "active":
        return "上班中"
    if str(value) == "paused":
        return "休息中"
    return str(value or "-")

def _prompt_type_label(value):
    mapping = {
        "persona": "员工技能 Prompt",
        "planning": "目标拆解 Prompt",
        "kpi": "动态 KPI Prompt",
        "schedule": "工作排班 Prompt",
    }
    return mapping.get(str(value), str(value or "-"))

def _prompt_scope_label(value):
    mapping = {
        "all": "通用",
        "seeding": "仅种草员工",
        "lead_generation": "仅引流员工",
        "strategy_lead": "仅分析",
    }
    return mapping.get(str(value), str(value or "-"))

def _default_employee_work_time_blocks():
    return [
        {"start": "09:00", "end": "12:00"},
        {"start": "14:00", "end": "18:00"},
    ]

def _normalize_work_time_blocks(blocks):
    normalized = []
    if not isinstance(blocks, list):
        return normalized
    for block in blocks:
        if not isinstance(block, dict):
            continue
        start_h, start_m = _parse_hhmm(block.get("start", "09:00"), 9, 0)
        end_h, end_m = _parse_hhmm(block.get("end", "12:00"), 12, 0)
        start_text = f"{start_h:02d}:{start_m:02d}"
        end_text = f"{end_h:02d}:{end_m:02d}"
        if start_text >= end_text:
            continue
        normalized.append(
            {
                "start": start_text,
                "end": end_text,
            }
        )
    return normalized

def _format_work_time_blocks(blocks):
    normalized = _normalize_work_time_blocks(blocks)
    if not normalized:
        return "未设置"
    return " / ".join(f"{item['start']}-{item['end']}" for item in normalized)

def _summarize_text(text, max_chars=32):
    value = str(text or "").strip()
    if not value:
        return "-"
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "..."

def _safe_index(options, value, fallback=0):
    try:
        return list(options).index(value)
    except ValueError:
        return fallback

def _default_employee_prompts(username):
    now = _now_str()
    return [
        {
            "id": "default_persona_seeding",
            "dashboard_user": str(username),
            "name": "默认种草员工技能 Prompt",
            "prompt_type": "persona",
            "employee_type_scope": "seeding",
            "content": textwrap.dedent(
                """
                你现在的角色是一名资深的小红书种草员工。你的职责不是脱离系统从零写评论，而是先使用“话术管理”里现成的种草话术模板，再结合当前笔记内容、用户语气和场景做轻微微调。

                执行准则：
                1. 优先选择分类为“种草型”的话术模板；如果没有完全匹配的模板，再从“通用型”模板里选择最接近种草目标的一条。
                2. 微调必须继续保持“种草”方向，重点补充使用体验、改善感受、适配人群和使用场景，不要改写成引流、私聊或强转化话术。
                3. 语气要像真实用户，先共情，再给建议，避免生硬推销和参数堆砌。
                4. 评论需要像真人书写，允许有轻微口语化和自然停顿，但不要刻意夸张。
                5. 不主动索要私信，不留联系方式，不说平台敏感词。
                6. 如果当前模板和目标场景明显不匹配，应优先更换模板，而不是大幅重写模板内容。
                7. 你的目标不是立刻成交，而是让目标用户产生兴趣，愿意自己进一步了解。
                """
            ).strip(),
            "status": "active",
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "default_persona_lead_generation",
            "dashboard_user": str(username),
            "name": "默认引流员工技能 Prompt",
            "prompt_type": "persona",
            "employee_type_scope": "lead_generation",
            "content": textwrap.dedent(
                """
                你现在的角色是一名小红书引流员工。你的职责不是脱离系统从零写评论，而是先使用“话术管理”里现成的引流向话术模板，再结合当前笔记语境和目标用户状态做轻微微调。

                执行准则：
                1. 优先选择分类为“截流型”或“问询型”的话术模板；如果没有完全匹配的模板，再从“通用型”模板里选择最接近引流目标的一条。
                2. 微调必须继续保持“引流”方向，重点制造信息差、保留钩子、引导进一步互动，不要改写成纯种草分享或完整解答。
                3. 语气像懂行的过来人，只说关键结论，不一次把所有答案讲完。
                4. 评论要让用户愿意继续追问，但不要在评论区直接完成转化闭环。
                5. 严格规避敏感词，不直接留联系方式，不使用明显导流表述。
                6. 优先评论真实求助帖、犹豫帖、求推荐帖，避免对明显同行或广告内容出手。
                7. 如果当前模板和目标场景明显不匹配，应优先更换模板，而不是大幅重写模板内容。
                8. 你的目标是让用户产生继续了解的欲望，而不是在评论区完成完整转化。
                """
            ).strip(),
            "status": "active",
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "default_persona_strategy_lead",
            "dashboard_user": str(username),
            "name": "默认分析技能 Prompt",
            "prompt_type": "persona",
            "employee_type_scope": "strategy_lead",
            "content": textwrap.dedent(
                """
                你现在的角色是一名小红书分析。你的职责不是亲自评论，而是复盘团队执行数据并提出可执行的优化策略。

                输出要求：
                1. 明确给出当前最有效的话术方向、目标帖子类型、评论时间段。
                2. 明确指出表现较差的做法，并解释原因（资源、时段、模板、账号状态或风险）。
                3. 建议新增或下线哪些话术模板，并给出优先级。
                4. 结论必须引用当天数据证据（上评率、失败率、帖子点赞、评论点赞、回溯结果）。
                5. 建议要能直接执行，避免空泛口号。
                """
            ).strip(),
            "status": "active",
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "default_planning_all",
            "dashboard_user": str(username),
            "name": "默认目标拆解 Prompt",
            "prompt_type": "planning",
            "employee_type_scope": "all",
            "content": textwrap.dedent(
                """
                你是一名擅长任务规划的小红书运营分析员。老板给你的输入只有“目标描述”，你需要先理解目标，再自行拆解成当天可执行的搜索任务。

                请根据目标描述输出一个 JSON，对象中必须包含：
                1. search_keywords: 5 个左右可直接用于小红书搜索的关键词，要求覆盖同义表达、情绪表达和场景表达。
                2. filter_condition: 一段给爬虫 AI 粗筛使用的自然语言条件，说明该保留哪些帖子、排除哪些帖子。
                3. sort_type: 仅能返回“综合排序”或“最新”。
                4. reasoning: 简要解释为什么这样拆词。

                输出要求：
                - 关键词必须贴近真实用户表达，不要官方化。
                - 过滤条件要尽量排除同行广告、机构贴、纯营销贴。
                - 如果目标描述比较抽象，要主动补出用户的典型困惑和搜索习惯。
                """
            ).strip(),
            "status": "active",
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "default_kpi_all",
            "dashboard_user": str(username),
            "name": "默认动态 KPI Prompt",
            "prompt_type": "kpi",
            "employee_type_scope": "all",
            "content": textwrap.dedent(
                """
                你是一名谨慎的运营主管，需要结合当前资源动态制定今天的工作量，不允许盲目追求高 KPI。

                你会拿到以下上下文：
                - 目标描述
                - 可用爬取账号数量及状态
                - 可用评论账号数量及状态
                - 评论策略包的每日上限、小时上限、活跃时间段

                请输出一个 JSON，对象中必须包含：
                1. target_crawl_count: 今日建议抓取多少条笔记
                2. target_comment_count: 今日建议评论多少条
                3. risk_level: low / medium / high
                4. reasoning: 一段解释，说明为什么今天要这样定目标

                决策原则：
                - 评论目标不能脱离评论账号和策略包能力上限。
                - 如果账号异常、资源不足、策略包过紧，需要主动下调目标。
                - 如果今天更适合做搜集而非评论，也要明确说明。
                """
            ).strip(),
            "status": "active",
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "default_schedule_all",
            "dashboard_user": str(username),
            "name": "默认工作排班 Prompt",
            "prompt_type": "schedule",
            "employee_type_scope": "all",
            "content": textwrap.dedent(
                """
                你是一名拟人化执行调度员，需要把今天的目标任务合理分散到员工的工作时间窗口内，避免看起来像机器批量操作。

                你会拿到以下上下文：
                - 工作时间段列表
                - 今日评论目标数量
                - 评论策略包限制
                - 当前时间

                请输出一个 JSON 数组，每项包含：
                1. execute_at: 计划执行时间，格式 YYYY-MM-DD HH:MM:SS
                2. action_type: crawl / comment / review
                3. reason: 解释为什么安排在这个时间点

                决策原则：
                - 不要把任务挤在同一分钟。
                - 在工作时间段内自然分布，保留随机性和间隔感。
                - 如果总目标过大，要主动拆成多个时间窗口逐步完成。
                """
            ).strip(),
            "status": "active",
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        },
    ]

def load_employee_prompts(username):
    os.makedirs(os.path.dirname(EMPLOYEE_PROMPTS_FILE), exist_ok=True)
    defaults = _default_employee_prompts(username)
    default_templates = {
        str(item.get("id", "")): item
        for item in defaults
    }
    try:
        if os.path.exists(EMPLOYEE_PROMPTS_FILE):
            with open(EMPLOYEE_PROMPTS_FILE, "r", encoding="utf-8") as f:
                all_prompts = json.load(f)
            if not isinstance(all_prompts, list):
                all_prompts = []
        else:
            all_prompts = []
    except Exception:
        all_prompts = []

    user_prompts = [
        p for p in all_prompts
        if str(p.get("dashboard_user", "")) == str(username)
    ]
    existing_ids = {str(p.get("id", "")) for p in user_prompts}
    changed = False
    for idx, item in enumerate(user_prompts):
        prompt_id = str(item.get("id", "")).strip()
        if prompt_id not in default_templates:
            continue
        template = default_templates[prompt_id]
        if (
            str(item.get("name", "")).strip() != str(template.get("name", "")).strip()
            or str(item.get("prompt_type", "")).strip() != str(template.get("prompt_type", "")).strip()
            or str(item.get("employee_type_scope", "")).strip() != str(template.get("employee_type_scope", "")).strip()
            or str(item.get("content", "")).strip() != str(template.get("content", "")).strip()
            or str(item.get("status", "active")).strip() != str(template.get("status", "active")).strip()
            or not bool(item.get("is_default"))
        ):
            user_prompts[idx] = {
                **dict(item),
                "name": template.get("name", ""),
                "prompt_type": template.get("prompt_type", "planning"),
                "employee_type_scope": template.get("employee_type_scope", "all"),
                "content": template.get("content", ""),
                "status": template.get("status", "active"),
                "is_default": True,
            }
            changed = True
    for default_prompt in defaults:
        if str(default_prompt["id"]) not in existing_ids:
            user_prompts.append(default_prompt)
            changed = True

    user_prompts = sorted(
        user_prompts,
        key=lambda item: (
            0 if item.get("is_default") else 1,
            str(item.get("prompt_type", "")),
            str(item.get("name", "")),
        ),
    )
    if changed or not os.path.exists(EMPLOYEE_PROMPTS_FILE):
        save_employee_prompts_for_user(username, user_prompts)
    return user_prompts

def save_employee_prompts_for_user(username, user_prompts):
    os.makedirs(os.path.dirname(EMPLOYEE_PROMPTS_FILE), exist_ok=True)
    try:
        if os.path.exists(EMPLOYEE_PROMPTS_FILE):
            with open(EMPLOYEE_PROMPTS_FILE, "r", encoding="utf-8") as f:
                all_prompts = json.load(f)
            if not isinstance(all_prompts, list):
                all_prompts = []
        else:
            all_prompts = []
    except Exception:
        all_prompts = []

    default_templates = {
        str(item["id"]): item
        for item in _default_employee_prompts(username)
    }
    normalized_prompts = []
    seen_ids = set()
    for item in user_prompts:
        if not isinstance(item, dict):
            continue
        prompt_id = str(item.get("id", "")).strip()
        if not prompt_id:
            prompt_id = f"prompt_{int(time.time() * 1000)}_{len(normalized_prompts)}"
        copied = dict(item)
        copied["id"] = prompt_id
        copied["dashboard_user"] = str(username)
        copied["name"] = str(copied.get("name", "未命名 Prompt")).strip() or "未命名 Prompt"
        normalized_prompt_type = str(copied.get("prompt_type", "planning")).strip() or "planning"
        copied["prompt_type"] = normalized_prompt_type if normalized_prompt_type in {"persona", "planning", "kpi", "schedule"} else "planning"
        normalized_scope = str(copied.get("employee_type_scope", "all")).strip() or "all"
        copied["employee_type_scope"] = normalized_scope if normalized_scope in set(EMPLOYEE_PROMPT_SCOPE_OPTIONS) else "all"
        copied["status"] = "inactive" if str(copied.get("status", "active")) == "inactive" else "active"
        copied["content"] = str(copied.get("content", "")).strip()
        copied["created_at"] = str(copied.get("created_at") or _now_str())
        copied["updated_at"] = _now_str()
        copied["is_default"] = bool(copied.get("is_default"))
        if prompt_id in default_templates:
            template = default_templates[prompt_id]
            copied["name"] = template["name"]
            copied["prompt_type"] = template["prompt_type"]
            copied["employee_type_scope"] = template["employee_type_scope"]
            copied["content"] = template["content"]
            copied["status"] = template["status"]
            copied["is_default"] = True
        normalized_prompts.append(copied)
        seen_ids.add(prompt_id)

    for prompt_id, template in default_templates.items():
        if prompt_id not in seen_ids:
            normalized_prompts.append(template)

    kept = [
        p for p in all_prompts
        if str(p.get("dashboard_user", "")) != str(username)
    ]
    kept.extend(normalized_prompts)
    with open(EMPLOYEE_PROMPTS_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def load_employee_profiles(username):
    os.makedirs(os.path.dirname(EMPLOYEE_PROFILES_FILE), exist_ok=True)
    try:
        if os.path.exists(EMPLOYEE_PROFILES_FILE):
            with open(EMPLOYEE_PROFILES_FILE, "r", encoding="utf-8") as f:
                all_profiles = json.load(f)
            if not isinstance(all_profiles, list):
                all_profiles = []
        else:
            all_profiles = []
    except Exception:
        all_profiles = []

    user_profiles = []
    for item in all_profiles:
        if str(item.get("dashboard_user", "")) != str(username):
            continue
        copied = dict(item)
        copied["work_time_blocks"] = _normalize_work_time_blocks(copied.get("work_time_blocks", []))
        user_profiles.append(copied)

    return sorted(
        user_profiles,
        key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""),
        reverse=True,
    )

def save_employee_profiles_for_user(username, user_profiles):
    os.makedirs(os.path.dirname(EMPLOYEE_PROFILES_FILE), exist_ok=True)
    try:
        if os.path.exists(EMPLOYEE_PROFILES_FILE):
            with open(EMPLOYEE_PROFILES_FILE, "r", encoding="utf-8") as f:
                all_profiles = json.load(f)
            if not isinstance(all_profiles, list):
                all_profiles = []
        else:
            all_profiles = []
    except Exception:
        all_profiles = []

    normalized_profiles = []
    for item in user_profiles:
        if not isinstance(item, dict):
            continue
        profile_id = str(item.get("id", "")).strip() or f"employee_{int(time.time() * 1000)}_{len(normalized_profiles)}"
        copied = dict(item)
        copied["id"] = profile_id
        copied["dashboard_user"] = str(username)
        copied["name"] = str(copied.get("name", "未命名员工")).strip() or "未命名员工"
        copied["status"] = "paused" if str(copied.get("status", "active")) == "paused" else "active"
        normalized_employee_type = str(copied.get("employee_type", "seeding")).strip()
        copied["employee_type"] = normalized_employee_type if normalized_employee_type in set(EMPLOYEE_TYPE_OPTIONS) else "seeding"
        copied["goal_description"] = str(copied.get("goal_description", "")).strip()
        copied["work_time_blocks"] = _normalize_work_time_blocks(copied.get("work_time_blocks", []))
        copied["assigned_crawl_accounts"] = [
            str(value).strip()
            for value in copied.get("assigned_crawl_accounts", [])
            if str(value).strip()
        ]
        copied["assigned_comment_accounts"] = [
            str(value).strip()
            for value in copied.get("assigned_comment_accounts", [])
            if str(value).strip()
        ]
        copied["assigned_strategy_package_ids"] = [
            str(value).strip()
            for value in copied.get("assigned_strategy_package_ids", [])
            if str(value).strip()
        ]
        copied["persona_prompt_id"] = str(copied.get("persona_prompt_id", "")).strip()
        copied["planning_prompt_id"] = str(copied.get("planning_prompt_id", "")).strip()
        copied["kpi_prompt_id"] = str(copied.get("kpi_prompt_id", "")).strip()
        copied["schedule_prompt_id"] = str(copied.get("schedule_prompt_id", "")).strip()
        copied["analysis_scope_mode"] = (
            str(copied.get("analysis_scope_mode", "all")).strip()
            if str(copied.get("analysis_scope_mode", "all")).strip() in set(STRATEGY_LEAD_SCOPE_OPTIONS)
            else "all"
        )
        copied["analysis_employee_ids"] = [
            str(value).strip()
            for value in copied.get("analysis_employee_ids", [])
            if str(value).strip()
        ]
        copied["analysis_frequency"] = (
            "weekly" if str(copied.get("analysis_frequency", "daily")).strip() == "weekly" else "daily"
        )
        copied["notes"] = str(copied.get("notes", "")).strip()
        copied["created_at"] = str(copied.get("created_at") or _now_str())
        copied["updated_at"] = _now_str()
        normalized_profiles.append(copied)

    kept = [
        p for p in all_profiles
        if str(p.get("dashboard_user", "")) != str(username)
    ]
    kept.extend(normalized_profiles)
    with open(EMPLOYEE_PROFILES_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

def _get_prompt_by_id(prompts, prompt_id):
    return next(
        (item for item in prompts if str(item.get("id", "")) == str(prompt_id)),
        None,
    )

def _prompt_matches_employee_type(prompt, employee_type):
    scope = str(prompt.get("employee_type_scope", "all"))
    return scope in ("all", str(employee_type))

def _prompt_sort_key(prompt, employee_type):
    scope = str(prompt.get("employee_type_scope", "all"))
    scope_rank = 0 if scope == str(employee_type) else 1 if scope == "all" else 2
    default_rank = 0 if prompt.get("is_default") else 1
    return (scope_rank, default_rank, str(prompt.get("name", "")))

def _get_prompt_candidates(prompts, prompt_type, employee_type, active_only=True):
    matches = []
    for prompt in prompts:
        if str(prompt.get("prompt_type", "")) != str(prompt_type):
            continue
        if not _prompt_matches_employee_type(prompt, employee_type):
            continue
        if active_only and str(prompt.get("status", "active")) != "active":
            continue
        matches.append(prompt)
    return sorted(matches, key=lambda item: _prompt_sort_key(item, employee_type))

def _resolve_prompt_id(prompts, prompt_type, employee_type, selected_id=None):
    candidates = _get_prompt_candidates(prompts, prompt_type, employee_type, active_only=True)
    if not candidates:
        candidates = _get_prompt_candidates(prompts, prompt_type, employee_type, active_only=False)
    candidate_ids = [str(item.get("id", "")) for item in candidates]
    if selected_id and str(selected_id) in candidate_ids:
        return str(selected_id)
    if candidates:
        return str(candidates[0].get("id", ""))
    return ""

def _prompt_option_label(prompt):
    status_text = "默认" if prompt.get("is_default") else ("启用" if str(prompt.get("status", "active")) == "active" else "停用")
    return f"{prompt.get('name', '-')} | {_prompt_scope_label(prompt.get('employee_type_scope', 'all'))} | {status_text}"

def _build_employee_work_time_editor(prefix, blocks):
    preset_items = [
        ("上午", "09:00", "12:00"),
        ("下午", "14:00", "18:00"),
        ("晚间", "20:00", "22:30"),
    ]
    normalized = _normalize_work_time_blocks(blocks)
    default_blocks = normalized or _default_employee_work_time_blocks()
    results = []

    st.caption("勾选员工可工作的时间段，后续排班 Prompt 会在这些窗口内安排动作。")
    for idx, (label, default_start, default_end) in enumerate(preset_items):
        existing = default_blocks[idx] if idx < len(default_blocks) else {"start": default_start, "end": default_end}
        enabled_default = idx < len(default_blocks)
        enabled = st.checkbox(label, value=enabled_default, key=f"{prefix}_enabled_{idx}")
        cols = st.columns(2)
        start_value = datetime.strptime(existing.get("start", default_start), "%H:%M").time()
        end_value = datetime.strptime(existing.get("end", default_end), "%H:%M").time()
        start_time = cols[0].time_input(
            f"{label}开始",
            value=start_value,
            key=f"{prefix}_start_{idx}",
            disabled=not enabled,
        )
        end_time = cols[1].time_input(
            f"{label}结束",
            value=end_value,
            key=f"{prefix}_end_{idx}",
            disabled=not enabled,
        )
        if enabled:
            start_text = start_time.strftime("%H:%M")
            end_text = end_time.strftime("%H:%M")
            if start_text < end_text:
                results.append({"start": start_text, "end": end_text})
    return _normalize_work_time_blocks(results)

def _parse_hhmm(text, fallback_h=8, fallback_m=30):
    try:
        value = str(text).strip()
        if ":" not in value:
            return fallback_h, fallback_m
        h_str, m_str = value.split(":", 1)
        hour = max(0, min(23, int(h_str)))
        minute = max(0, min(59, int(m_str)))
        return hour, minute
    except Exception:
        return fallback_h, fallback_m

def _to_dt(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _align_to_active_window(dt_value, active_start, active_end):
    start_h, start_m = _parse_hhmm(active_start, 8, 30)
    end_h, end_m = _parse_hhmm(active_end, 23, 0)
    start_dt = dt_value.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end_dt = dt_value.replace(hour=end_h, minute=end_m, second=0, microsecond=0)

    # Support both same-day window (08:30-23:00) and cross-day window (22:00-06:00).
    if start_dt <= end_dt:
        if dt_value < start_dt:
            return start_dt
        if dt_value > end_dt:
            next_day = dt_value + timedelta(days=1)
            return next_day.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
        return dt_value

    # Cross-day: active if >= start OR <= end.
    if dt_value >= start_dt or dt_value <= end_dt:
        return dt_value
    return start_dt

def _count_events_for_slot(events, target_dt):
    day_count = 0
    hour_count = 0
    for item in events:
        event_dt = _to_dt(item.get("scheduled_for"))
        if not event_dt:
            continue
        if event_dt.date() == target_dt.date():
            day_count += 1
        if (
            event_dt.year == target_dt.year
            and event_dt.month == target_dt.month
            and event_dt.day == target_dt.day
            and event_dt.hour == target_dt.hour
        ):
            hour_count += 1
    return day_count, hour_count


def load_comment_schedule_records(username, strategy_package_id=None):
    jobs = load_comment_jobs(username)
    if jobs:
        records = jobs
        if strategy_package_id:
            records = [
                item for item in records
                if str(item.get("strategy_package_id", "")) == str(strategy_package_id)
            ]
        return [
            item for item in records
            if str(item.get("status", "")).lower() not in {"send_failed", "cancelled"}
        ]

    all_events = load_comment_strategy_events(username)
    if strategy_package_id:
        all_events = [
            e for e in all_events
            if str(e.get("strategy_package_id", "")) == str(strategy_package_id)
        ]
    return [
        e for e in all_events
        if str(e.get("status", "")).lower() not in {"failed", "error"}
    ]


def compute_comment_schedule(username, now_dt=None, strategy_override=None, strategy_package_id=None):
    now_dt = now_dt or datetime.now()
    strategy = strategy_override or load_comment_strategy(username)
    events = load_comment_schedule_records(username, strategy_package_id=strategy_package_id)

    daily_limit = max(1, int(strategy.get("daily_limit", 120)))
    hourly_limit = max(1, int(strategy.get("hourly_limit", 20)))
    jitter_min = max(0, int(strategy.get("jitter_min_sec", 45)))
    jitter_max = max(jitter_min, int(strategy.get("jitter_max_sec", 180)))

    latest_dt = None
    for item in events:
        event_dt = _to_dt(item.get("scheduled_for"))
        if event_dt and (latest_dt is None or event_dt > latest_dt):
            latest_dt = event_dt

    base_dt = now_dt if latest_dt is None else max(now_dt, latest_dt)
    jitter_sec = random.randint(jitter_min, jitter_max)
    candidate = base_dt + timedelta(seconds=jitter_sec)
    candidate = _align_to_active_window(
        candidate,
        strategy.get("active_start", "08:30"),
        strategy.get("active_end", "23:00"),
    )

    guard = 0
    while guard < 200:
        guard += 1
        day_count, hour_count = _count_events_for_slot(events, candidate)
        if day_count >= daily_limit:
            next_day = candidate + timedelta(days=1)
            candidate = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
            candidate = _align_to_active_window(
                candidate,
                strategy.get("active_start", "08:30"),
                strategy.get("active_end", "23:00"),
            )
            continue
        if hour_count >= hourly_limit:
            candidate = (candidate.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            candidate = _align_to_active_window(
                candidate,
                strategy.get("active_start", "08:30"),
                strategy.get("active_end", "23:00"),
            )
            continue
        break

    delayed = candidate > now_dt + timedelta(seconds=2)
    reason = "命中策略限制，已自动顺延执行时间。" if delayed else "当前满足策略，可立即执行。"
    return {
        "strategy": strategy,
        "events": events,
        "scheduled_dt": candidate,
        "jitter_sec": jitter_sec,
        "delayed": delayed,
        "reason": reason,
    }

def normalize_cookie_input(raw_text):
    """Normalize cookie text from header string or DevTools table copy."""
    if not raw_text:
        return ""

    text = raw_text.strip()
    if not text:
        return ""

    text = re.sub(r"^\s*cookie\s*:\s*", "", text, flags=re.IGNORECASE)
    pairs = []

    # Case 1: already a Cookie header string.
    if ";" in text and "=" in text and "\n" not in text:
        for part in text.split(";"):
            segment = part.strip()
            if "=" not in segment:
                continue
            name, value = segment.split("=", 1)
            name = name.strip()
            value = value.strip()
            if name and value:
                pairs.append((name, value))
    else:
        # Case 2: pasted rows from DevTools cookies table.
        for line in text.splitlines():
            row = line.strip()
            if not row:
                continue

            if "=" in row and "\t" not in row:
                name, value = row.split("=", 1)
                name = name.strip()
                value = value.strip()
                if name and value:
                    pairs.append((name, value))
                continue

            # Split by tab, preserving empty strings to maintain column alignment
            cols = row.split("\t")
            if len(cols) >= 2:
                name = cols[0].strip()
                value = cols[1].strip()
                
                # Skip the header row from DevTools copy
                if name.lower() == "name" and value.lower() == "value":
                    continue
                    
                if name:
                    pairs.append((name, value))

    if not pairs:
        return ""

    # Deduplicate by key while preserving insertion order.
    normalized = {}
    for key, value in pairs:
        normalized[key] = value

    return "; ".join(f"{k}={v}" for k, v in normalized.items())

def load_data_for_username(username):
    conn = get_connection()
    query = """
    SELECT 
        note_id,
        title,
        nickname,
        user_id,
        time,
        liked_count,
        collected_count,
        comment_count,
        note_url,
        xsec_token,
        desc,
        image_list,
        comment_status,
        source_keyword,
        account_name,
        add_ts,
        last_modify_ts
    FROM xhs_note
    WHERE dashboard_user = ?
    ORDER BY time DESC
    """
    try:
        df = pd.read_sql_query(query, conn, params=(str(username),))
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

def load_data():
    return load_data_for_username(st.session_state.username)

def get_comments(note_id, limit=50):
    conn = get_connection()
    query = """
    SELECT 
        comment_id,
        user_id,
        nickname,
        content,
        create_time,
        like_count,
        ip_location,
        sub_comment_count,
        parent_comment_id
    FROM xhs_note_comment
    WHERE note_id = ?
    ORDER BY like_count DESC
    LIMIT ?
    """
    try:
        df = pd.read_sql_query(query, conn, params=(note_id, limit))
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

def format_timestamp(ts):
    if not ts:
        return ""
    try:
        # Check if timestamp is in milliseconds (13 digits)
        if len(str(ts)) == 13:
            return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts)

def parse_metric_value(value):
    """Parse metric strings like '123', '1.2万', '3,456+' to float."""
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace(",", "").replace("+", "")
    multiplier = 1.0
    if text.endswith("万"):
        multiplier = 10000.0
        text = text[:-1]
    elif text.endswith("亿"):
        multiplier = 100000000.0
        text = text[:-1]
    try:
        return float(text) * multiplier
    except Exception:
        return 0.0

def to_datetime_from_ts(ts):
    """Convert seconds/milliseconds timestamp to datetime."""
    if ts is None or ts == "":
        return None
    try:
        ts_int = int(float(ts))
        if ts_int <= 0:
            return None
        if ts_int > 10**12:
            ts_int = ts_int / 1000
        return datetime.fromtimestamp(ts_int)
    except Exception:
        return None

def parse_image_list(raw_value):
    """Best effort parse for image_list stored as JSON/text."""
    if raw_value is None:
        return []
    text = str(raw_value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if x]
    except Exception:
        pass
    # Fallback: comma/newline separated URLs or python-list-like text.
    cleaned = text.strip("[]")
    parts = re.split(r"[\n,]", cleaned)
    urls = []
    for part in parts:
        item = part.strip().strip("'").strip('"')
        if item.startswith("http"):
            urls.append(item)
    return urls

def normalize_comment_id(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "none":
        return ""
    return text

def comment_preview_text(content, limit=48):
    text = str(content or "").replace("\n", " ").strip()
    if not text:
        return "（空评论）"
    return text[:limit] + ("..." if len(text) > limit else "")

def build_comment_target_option_key(comment, fallback_index):
    comment_id = normalize_comment_id(comment.get("comment_id"))
    if comment_id:
        return f"comment:{comment_id}"
    user_id = normalize_comment_id(comment.get("user_id"))
    if user_id:
        return f"user:{user_id}"
    return ""

def build_comment_target_options(comments):
    option_keys = []
    option_map = {}
    for idx, comment in enumerate(comments, start=1):
        option_key = build_comment_target_option_key(comment, idx)
        if not option_key:
            continue
        option_keys.append(option_key)
        option_map[option_key] = {
            "option_key": option_key,
            "comment_id": normalize_comment_id(comment.get("comment_id")),
            "user_id": normalize_comment_id(comment.get("user_id")),
            "nickname": str(comment.get("nickname") or "匿名用户"),
            "preview": comment_preview_text(comment.get("content")),
            "content": str(comment.get("content") or ""),
            "create_time": comment.get("create_time"),
            "parent_comment_id": normalize_comment_id(comment.get("parent_comment_id")),
        }
    return option_keys, option_map

def format_comment_target_label(target):
    level_text = "子评论" if target.get("parent_comment_id") else "一级评论"
    nickname = target.get("nickname") or "匿名用户"
    create_time = format_timestamp(target.get("create_time"))
    preview = target.get("preview") or "（空评论）"
    return f"{level_text}｜{nickname}｜{create_time}｜{preview}"

def clear_comment_dialog_state():
    st.session_state.selected_comment_note_id = None
    st.session_state.selected_comment_mode = "note"
    st.session_state.selected_comment_target_key = None
    st.session_state.selected_comment_target_id = None
    st.session_state.selected_comment_target_user_id = None
    st.session_state.selected_comment_target_nickname = ""
    st.session_state.selected_comment_target_preview = ""

def clear_note_overlay_state():
    st.session_state.selected_note_id = None
    clear_comment_dialog_state()

NAV_QUERY_KEY = "nav"
NAV_PAGE_KEY_TO_LABEL = {
    "guide": "电子员工使用手册",
    "employees": "员工管理",
    "boss": "老板看板",
    "crawl_accounts": "爬取账号",
    "comment_accounts": "评论账号",
    "templates": "话术管理",
    "strategy": "评论策略",
    "notes_capture": "笔记收录",
    "notes_list": "笔记展示",
    "comment_execution": "执行中心",
}
NAV_PAGE_LABEL_TO_KEY = {label: key for key, label in NAV_PAGE_KEY_TO_LABEL.items()}
NAV_PAGE_LABEL_ALIASES = {
    "新手导览": "电子员工使用手册",
    "电子员工导览": "电子员工使用手册",
    "评论执行中心": "执行中心",
}

def normalize_dashboard_page_label(page_name):
    normalized = str(page_name or "").strip()
    return NAV_PAGE_LABEL_ALIASES.get(normalized, normalized)

def dashboard_page_from_query_value(query_value):
    normalized = str(query_value or "").strip()
    if normalized in NAV_PAGE_KEY_TO_LABEL:
        return NAV_PAGE_KEY_TO_LABEL[normalized]
    return normalize_dashboard_page_label(normalized)

def sync_dashboard_page_query(page_name):
    normalized_page = normalize_dashboard_page_label(page_name)
    query_value = NAV_PAGE_LABEL_TO_KEY.get(normalized_page, normalized_page)
    current_value = str(st.query_params.get(NAV_QUERY_KEY, "") or "").strip()
    if current_value != query_value:
        st.query_params[NAV_QUERY_KEY] = query_value

def resolve_dashboard_page(page_options, default_page):
    query_page = dashboard_page_from_query_value(st.query_params.get(NAV_QUERY_KEY, ""))
    if query_page in page_options:
        return query_page
    session_page = normalize_dashboard_page_label(st.session_state.get("dashboard_page"))
    if session_page in page_options:
        return session_page
    return default_page

def switch_dashboard_page(page_name):
    clear_note_overlay_state()
    normalized_page = normalize_dashboard_page_label(page_name)
    st.session_state.dashboard_page = normalized_page
    sync_dashboard_page_query(normalized_page)
    st.rerun()

def render_beginner_guide_page():
    st.header("欢迎来到电子员工工作台")
    st.caption("在这里，你不是亲自做运营，而是在管理一支自动化运营团队。")

    st.markdown("## 创建电子员工，让它替你完成原本需要多个运营人力才能完成的事")
    st.write(
        "你只需要设定目标、绑定账号、配置策略和工作时间，电子员工就会自动找内容、执行互动、回看结果，并把日报交到你面前。"
    )
    st.success("你负责定方向，电子员工负责干活。")

    st.markdown("### 电子员工能替你做什么")
    st.markdown("**自动找目标内容**")
    st.caption("持续收录和筛选值得跟进的笔记，不用人工一条条去搜。")
    st.markdown("**自动执行评论动作**")
    st.caption("按你设定的话术和策略执行互动，不用人工重复复制粘贴。")
    st.markdown("**自动回溯结果表现**")
    st.caption("自动查看任务状态、账号表现和上评结果，不用人工来回对账。")
    st.markdown("**自动生成老板日报**")
    st.caption("把 KPI 判断、关键流水和日报汇总出来，不用临时拼报表。")
    st.info("从“堆人做运营”，变成“一个人管理一支自动化团队”。")

    st.markdown("---")
    st.markdown("### 电子员工是怎么开始替你工作的？")
    step1, step2, step3, step4 = st.columns(4)
    with step1:
        st.markdown("#### 1. 给它一个岗位")
        st.write("先告诉它你想做什么业务，想触达什么人，最终想拿到什么结果。")
    with step2:
        st.markdown("#### 2. 给它配资源")
        st.write("给它绑定账号、话术、策略和工作时间，让它具备真正开工的条件。")
    with step3:
        st.markdown("#### 3. 让它自动执行")
        st.write("它会自己去找内容、筛机会、排任务、执行互动并回看结果。")
    with step4:
        st.markdown("#### 4. 你只看结果和调整")
        st.write("老板看板会把今天做了什么、效果如何、哪里要优化直接告诉你。")
    st.success("你管目标和规则，电子员工管执行。")

    st.markdown("---")
    st.markdown("### 这个后台里的每个模块，本质上都在服务电子员工")
    module_left, module_right = st.columns(2)
    with module_left:
        st.markdown("**员工管理**：给电子员工定岗位、定目标、定工作方式。")
        st.markdown("**老板看板**：看电子员工今天干了什么、结果怎么样。")
        st.markdown("**爬取账号**：给电子员工配找内容的账号资源。")
        st.markdown("**评论账号**：给电子员工配执行互动的账号资源。")
        st.markdown("**话术管理**：教电子员工在不同场景下怎么说。")
    with module_right:
        st.markdown("**评论策略**：规定电子员工什么时候做、做多少、做得多像真人。")
        st.markdown("**笔记收录**：电子员工寻找目标内容的入口。")
        st.markdown("**笔记展示**：电子员工判断哪些内容值得跟进的工作现场。")
        st.markdown("**执行中心**：看任务有没有真正发出，以及结果有没有达标。")
    st.caption("你看到的不是一堆功能按钮，而是一整套电子员工的工作系统。")

    st.markdown("---")
    st.markdown("### 第一次使用，建议你这样开始")
    start1, start2, start3 = st.columns(3)
    with start1:
        st.markdown("#### 先创建电子员工")
        st.write("先把岗位和目标定义清楚，再逐步补资源。")
        if st.button("去员工管理", key="guide_go_employee_page", type="primary", use_container_width=True):
            switch_dashboard_page("员工管理")
    with start2:
        st.markdown("#### 再给它配账号和策略")
        st.write("让电子员工有账号可用、有话术可说、有策略可跑。")
        if st.button("去配爬取账号", key="guide_go_crawl_accounts", use_container_width=True):
            switch_dashboard_page("爬取账号")
        if st.button("去配评论账号", key="guide_go_comment_accounts", use_container_width=True):
            switch_dashboard_page("评论账号")
        if st.button("去配话术策略", key="guide_go_strategy_setup", use_container_width=True):
            switch_dashboard_page("评论策略")
    with start3:
        st.markdown("#### 最后看它开始工作")
        st.write("去执行中心看动作，去老板看板看结果。")
        if st.button("去执行中心", key="guide_go_execution_center", type="primary", use_container_width=True):
            switch_dashboard_page("执行中心")
        if st.button("去老板看板", key="guide_go_boss_dashboard", use_container_width=True):
            switch_dashboard_page("老板看板")
    st.warning("你第一次进来最重要的，不是自己下场操作，而是先把第一个电子员工搭起来。")

def open_note_comment_dialog(note_id):
    st.session_state.selected_comment_note_id = str(note_id)
    st.session_state.selected_comment_mode = "note"
    st.session_state.selected_comment_target_key = None
    st.session_state.selected_comment_target_id = None
    st.session_state.selected_comment_target_user_id = None
    st.session_state.selected_comment_target_nickname = ""
    st.session_state.selected_comment_target_preview = ""

def open_reply_comment_dialog(note_id, comment, fallback_index):
    option_key = build_comment_target_option_key(comment, fallback_index)
    st.session_state.selected_comment_note_id = str(note_id)
    st.session_state.selected_comment_mode = "reply"
    st.session_state.selected_comment_target_key = option_key
    st.session_state.selected_comment_target_id = normalize_comment_id(comment.get("comment_id"))
    st.session_state.selected_comment_target_user_id = normalize_comment_id(comment.get("user_id"))
    st.session_state.selected_comment_target_nickname = str(comment.get("nickname") or "匿名用户")
    st.session_state.selected_comment_target_preview = comment_preview_text(comment.get("content"))

def format_comment_event_target(event):
    target_type = str(event.get("target_type") or "note")
    if target_type == "reply":
        nickname = str(event.get("target_nickname") or "").strip()
        preview = str(event.get("target_comment_preview") or "").strip()
        target_text = preview or str(event.get("target_comment_id") or event.get("target_user_id") or "-")
        if nickname:
            target_text = f"@{nickname}｜{target_text}"
        return f"回复评论｜{target_text}"
    return "整帖评论"

def format_comment_event_status(status):
    mapping = {
        "sent": "已发送",
        "failed": "发送失败",
        "scheduled_placeholder": "占位排程",
        "queued": "排队中",
        "dispatching": "发送中",
        "sent_api_ok": "接口发送成功",
        "send_failed": "发送失败",
        "verify_pending": "待回溯",
        "verified_visible": "已确认上评",
        "verified_hidden": "未确认上评",
        "manual_review": "需人工复核",
        "cancelled": "已取消",
    }
    return mapping.get(str(status), str(status) or "-")

def _report_date_to_str(value):
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            pass
    text = str(value or "").strip()
    if not text:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")

def _report_day_bounds(report_date):
    report_date_text = _report_date_to_str(report_date)
    start_dt = datetime.strptime(report_date_text, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)
    return report_date_text, start_dt, end_dt

def _datetime_in_report_date(dt_value, report_date):
    if not dt_value:
        return False
    report_date_text, start_dt, end_dt = _report_day_bounds(report_date)
    _ = report_date_text
    return start_dt <= dt_value < end_dt

def _safe_json_loads(raw_value, fallback=None):
    fallback = {} if fallback is None else fallback
    if raw_value in (None, ""):
        return fallback
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except Exception:
        return fallback

def _coerce_non_negative_int(value, default=0):
    try:
        return max(0, int(float(value)))
    except Exception:
        return int(default)

def _normalize_risk_level(value):
    lowered = str(value or "").strip().lower()
    if lowered in {"low", "medium", "high"}:
        return lowered
    return "medium"

def _risk_level_label(level):
    mapping = {
        "low": "低风险",
        "medium": "中风险",
        "high": "高风险",
    }
    return mapping.get(_normalize_risk_level(level), "中风险")

def _risk_level_markdown(level):
    normalized = _normalize_risk_level(level)
    if normalized == "low":
        return ":green[低风险]"
    if normalized == "high":
        return ":red[高风险]"
    return ":orange[中风险]"

def _format_dt_display(value):
    dt_value = _to_dt(value) if not isinstance(value, datetime) else value
    if not dt_value:
        return "-"
    return dt_value.strftime("%Y-%m-%d %H:%M:%S")

def _format_token_usage(total_tokens):
    token_count = _coerce_non_negative_int(total_tokens)
    if token_count <= 0:
        return "0"
    return f"{token_count:,}"

def _build_note_datetime_series(df):
    if df.empty:
        return pd.Series(dtype="datetime64[ns]")
    add_series = df["add_ts"].apply(to_datetime_from_ts) if "add_ts" in df.columns else pd.Series([None] * len(df))
    modify_series = df["last_modify_ts"].apply(to_datetime_from_ts) if "last_modify_ts" in df.columns else pd.Series([None] * len(df))
    return add_series.where(add_series.notna(), modify_series)

def _filter_notes_by_report_date(notes_df, report_date):
    if notes_df.empty:
        return notes_df.copy()
    report_date_text, start_dt, end_dt = _report_day_bounds(report_date)
    _ = report_date_text
    note_dt = _build_note_datetime_series(notes_df)
    mask = note_dt.apply(lambda dt_value: bool(dt_value and start_dt <= dt_value < end_dt))
    return notes_df.loc[mask].copy()

def _event_time_for_timeline(event):
    for key in ("executed_at", "created_at", "scheduled_for"):
        dt_value = _to_dt(event.get(key))
        if dt_value:
            return dt_value
    return None

def _filter_events_by_report_date(events, report_date):
    filtered = []
    for item in events:
        if _datetime_in_report_date(_event_time_for_timeline(item), report_date):
            filtered.append(item)
    return filtered

def _json_dumps_compact(value):
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        return "{}"


def _hash_comment_content(value):
    text = str(value or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _normalize_comment_match_text(value):
    text = str(value or "")
    text = re.sub(r"\s+", " ", text).strip().lower()
    translation = str.maketrans({
        "，": ",",
        "。": ".",
        "！": "!",
        "？": "?",
        "：": ":",
        "；": ";",
        "（": "(",
        "）": ")",
        "【": "[",
        "】": "]",
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
        "\u3000": " ",
    })
    return text.translate(translation)


def format_comment_job_status(status):
    return COMMENT_JOB_STATUS_LABELS.get(str(status or "").strip(), str(status or "-"))


def format_comment_verdict(verdict):
    return COMMENT_VERIFICATION_VERDICT_LABELS.get(str(verdict or "").strip(), str(verdict or "-"))


def _read_task_records(query, params=()):
    conn = get_tasks_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df


def load_verifier_accounts(username, active_only=False):
    query = """
    SELECT *
    FROM verifier_accounts
    WHERE dashboard_user = ?
    """
    params = [str(username)]
    if active_only:
        query += " AND status = 'active'"
    query += " ORDER BY is_default DESC, updated_at DESC, id DESC"
    df = _read_task_records(query, params=params)
    return df.to_dict("records") if not df.empty else []


def get_default_verifier_account(username):
    accounts = load_verifier_accounts(username, active_only=True)
    if not accounts:
        return None
    explicit = next((item for item in accounts if int(item.get("is_default", 0) or 0) == 1), None)
    return explicit or accounts[0]


def upsert_verifier_account(username, account_id, account_name="", notes="", status="active", make_default=False):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    if not normalized_account_id:
        raise ValueError("验证账号ID不能为空，且不能使用默认运行时账号占位符。")
    normalized_status = "active" if str(status or "active") == "active" else "inactive"
    now = _now_str()
    conn = get_tasks_connection()
    try:
        row = conn.execute(
            "SELECT id, is_default FROM verifier_accounts WHERE dashboard_user = ? AND account_id = ?",
            (str(username), normalized_account_id),
        ).fetchone()
        if make_default:
            conn.execute(
                "UPDATE verifier_accounts SET is_default = 0, updated_at = ? WHERE dashboard_user = ?",
                (now, str(username)),
            )
        if row:
            conn.execute(
                """
                UPDATE verifier_accounts
                SET account_name = ?, notes = ?, status = ?, is_default = ?, updated_at = ?
                WHERE dashboard_user = ? AND account_id = ?
                """,
                (
                    str(account_name or normalized_account_id).strip() or normalized_account_id,
                    str(notes or "").strip(),
                    normalized_status,
                    1 if make_default else int(row[1] or 0),
                    now,
                    str(username),
                    normalized_account_id,
                ),
            )
        else:
            existing_default = conn.execute(
                "SELECT 1 FROM verifier_accounts WHERE dashboard_user = ? AND is_default = 1 LIMIT 1",
                (str(username),),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO verifier_accounts (
                    dashboard_user, account_id, account_name, status, is_default, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(username),
                    normalized_account_id,
                    str(account_name or normalized_account_id).strip() or normalized_account_id,
                    normalized_status,
                    1 if (make_default or not existing_default) else 0,
                    str(notes or "").strip(),
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return normalized_account_id


def set_default_verifier_account(username, account_id):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    if not normalized_account_id:
        raise ValueError("默认验证账号不能为空。")
    now = _now_str()
    conn = get_tasks_connection()
    try:
        conn.execute(
            "UPDATE verifier_accounts SET is_default = 0, updated_at = ? WHERE dashboard_user = ?",
            (now, str(username)),
        )
        conn.execute(
            """
            UPDATE verifier_accounts
            SET is_default = 1, updated_at = ?
            WHERE dashboard_user = ? AND account_id = ?
            """,
            (now, str(username), normalized_account_id),
        )
        conn.commit()
    finally:
        conn.close()


def delete_verifier_account(username, account_id):
    normalized_account_id = _normalize_mcp_account_id(account_id)
    if not normalized_account_id:
        return
    conn = get_tasks_connection()
    try:
        row = conn.execute(
            "SELECT is_default FROM verifier_accounts WHERE dashboard_user = ? AND account_id = ?",
            (str(username), normalized_account_id),
        ).fetchone()
        conn.execute(
            "DELETE FROM verifier_accounts WHERE dashboard_user = ? AND account_id = ?",
            (str(username), normalized_account_id),
        )
        if row and int(row[0] or 0) == 1:
            fallback = conn.execute(
                """
                SELECT account_id
                FROM verifier_accounts
                WHERE dashboard_user = ? AND status = 'active'
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (str(username),),
            ).fetchone()
            if fallback:
                conn.execute(
                    """
                    UPDATE verifier_accounts
                    SET is_default = 1, updated_at = ?
                    WHERE dashboard_user = ? AND account_id = ?
                    """,
                    (_now_str(), str(username), str(fallback[0])),
                )
        conn.commit()
    finally:
        conn.close()


def load_comment_jobs_df(username, statuses=None, limit=None, job_ids=None):
    query = """
    SELECT *
    FROM comment_jobs
    WHERE dashboard_user = ?
    """
    params = [str(username)]
    if statuses:
        normalized_statuses = [str(item) for item in statuses if str(item).strip()]
        if normalized_statuses:
            placeholders = ",".join(["?"] * len(normalized_statuses))
            query += f" AND status IN ({placeholders})"
            params.extend(normalized_statuses)
    if job_ids:
        normalized_job_ids = [int(item) for item in job_ids]
        if normalized_job_ids:
            placeholders = ",".join(["?"] * len(normalized_job_ids))
            query += f" AND id IN ({placeholders})"
            params.extend(normalized_job_ids)
    query += " ORDER BY datetime(COALESCE(scheduled_for, created_at)) DESC, id DESC"
    if limit:
        query += f" LIMIT {int(limit)}"
    return _read_task_records(query, params=params)


def load_comment_jobs(username, statuses=None, limit=None, job_ids=None):
    df = load_comment_jobs_df(username, statuses=statuses, limit=limit, job_ids=job_ids)
    return df.to_dict("records") if not df.empty else []


def get_comment_job(job_id, username=None):
    df = load_comment_jobs_df(username or st.session_state.username, job_ids=[job_id], limit=1)
    if df.empty:
        return None
    return df.to_dict("records")[0]


def create_comment_job(username, job_data):
    now = _now_str()
    payload_snapshot = dict(job_data.get("payload") or {})
    scheduled_for = str(job_data.get("scheduled_for") or now)
    conn = get_tasks_connection()
    try:
        cursor = conn.execute(
            """
            INSERT INTO comment_jobs (
                dashboard_user,
                note_id,
                note_title,
                note_url,
                xsec_token,
                target_type,
                target_comment_id,
                target_user_id,
                target_nickname,
                target_comment_preview,
                template_id,
                template_name,
                strategy_package_id,
                strategy_package_name,
                comment_account_id,
                comment_account_name,
                verifier_account_id,
                verifier_account_name,
                comment_content,
                comment_content_hash,
                status,
                scheduled_for,
                strategy_suggested_for,
                strategy_delayed,
                jitter_sec,
                payload_json,
                queued_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(username),
                str(job_data.get("note_id") or "").strip(),
                str(job_data.get("note_title") or "").strip(),
                str(job_data.get("note_url") or "").strip(),
                str(job_data.get("xsec_token") or "").strip(),
                str(job_data.get("target_type") or "note").strip() or "note",
                str(job_data.get("target_comment_id") or "").strip(),
                str(job_data.get("target_user_id") or "").strip(),
                str(job_data.get("target_nickname") or "").strip(),
                str(job_data.get("target_comment_preview") or "").strip(),
                str(job_data.get("template_id") or "").strip(),
                str(job_data.get("template_name") or "").strip(),
                str(job_data.get("strategy_package_id") or "").strip(),
                str(job_data.get("strategy_package_name") or "").strip(),
                str(job_data.get("comment_account_id") or MCP_DEFAULT_RUNTIME_ACCOUNT_ID),
                str(job_data.get("comment_account_name") or "").strip(),
                _normalize_mcp_account_id(job_data.get("verifier_account_id") or ""),
                str(job_data.get("verifier_account_name") or "").strip(),
                str(job_data.get("comment_content") or "").strip(),
                _hash_comment_content(job_data.get("comment_content") or ""),
                str(job_data.get("status") or "queued").strip() or "queued",
                scheduled_for,
                str(job_data.get("strategy_suggested_for") or scheduled_for),
                1 if bool(job_data.get("strategy_delayed")) else 0,
                _coerce_non_negative_int(job_data.get("jitter_sec", 0)),
                _json_dumps_compact(payload_snapshot),
                now,
                now,
                now,
            ),
        )
        job_id = int(cursor.lastrowid)
        conn.commit()
    finally:
        conn.close()
    return job_id


def update_comment_job_fields(job_id, field_values):
    updates = {str(k): v for k, v in (field_values or {}).items() if str(k).strip()}
    if not updates:
        return
    updates["updated_at"] = _now_str()
    assignments = ", ".join([f"{key} = ?" for key in updates.keys()])
    params = list(updates.values()) + [int(job_id)]
    conn = get_tasks_connection()
    try:
        conn.execute(f"UPDATE comment_jobs SET {assignments} WHERE id = ?", params)
        conn.commit()
    finally:
        conn.close()


def _update_comment_job_fields(job_id, field_values):
    # Backward-compatible alias for legacy internal callers.
    update_comment_job_fields(job_id, field_values)


def cancel_comment_jobs(username, job_ids):
    normalized_ids = [int(item) for item in job_ids if str(item).strip()]
    if not normalized_ids:
        return 0
    now = _now_str()
    placeholders = ",".join(["?"] * len(normalized_ids))
    conn = get_tasks_connection()
    try:
        cursor = conn.execute(
            f"""
            UPDATE comment_jobs
            SET status = 'cancelled', cancelled_at = ?, updated_at = ?
            WHERE dashboard_user = ?
              AND id IN ({placeholders})
              AND status IN ('queued', 'dispatching')
            """,
            [now, now, str(username), *normalized_ids],
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def requeue_comment_jobs(username, job_ids):
    normalized_ids = [int(item) for item in job_ids if str(item).strip()]
    if not normalized_ids:
        return 0
    now = _now_str()
    placeholders = ",".join(["?"] * len(normalized_ids))
    conn = get_tasks_connection()
    try:
        cursor = conn.execute(
            f"""
            UPDATE comment_jobs
            SET status = 'queued',
                scheduled_for = ?,
                started_at = NULL,
                cancelled_at = NULL,
                last_error = NULL,
                result_message = NULL,
                updated_at = ?
            WHERE dashboard_user = ?
              AND id IN ({placeholders})
              AND status IN ('send_failed', 'cancelled')
            """,
            [now, now, str(username), *normalized_ids],
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def schedule_comment_jobs_now(username, job_ids):
    normalized_ids = [int(item) for item in job_ids if str(item).strip()]
    if not normalized_ids:
        return 0
    now = _now_str()
    placeholders = ",".join(["?"] * len(normalized_ids))
    conn = get_tasks_connection()
    try:
        cursor = conn.execute(
            f"""
            UPDATE comment_jobs
            SET scheduled_for = ?, updated_at = ?
            WHERE dashboard_user = ?
              AND id IN ({placeholders})
              AND status = 'queued'
            """,
            [now, now, str(username), *normalized_ids],
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def claim_next_due_comment_job(username, specific_job_id=None):
    conn = get_tasks_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if specific_job_id is not None:
            row = conn.execute(
                """
                SELECT id
                FROM comment_jobs
                WHERE dashboard_user = ?
                  AND id = ?
                  AND status = 'queued'
                LIMIT 1
                """,
                (str(username), int(specific_job_id)),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id
                FROM comment_jobs
                WHERE dashboard_user = ?
                  AND status = 'queued'
                  AND datetime(COALESCE(scheduled_for, created_at)) <= datetime(?)
                ORDER BY datetime(COALESCE(scheduled_for, created_at)) ASC, id ASC
                LIMIT 1
                """,
                (str(username), _now_str()),
            ).fetchone()
        if not row:
            conn.commit()
            return None
        job_id = int(row[0])
        now = _now_str()
        cursor = conn.execute(
            """
            UPDATE comment_jobs
            SET status = 'dispatching',
                started_at = COALESCE(started_at, ?),
                updated_at = ?
            WHERE id = ?
              AND status = 'queued'
            """,
            (now, now, job_id),
        )
        conn.commit()
        if int(cursor.rowcount or 0) <= 0:
            return None
    finally:
        conn.close()
    return get_comment_job(job_id, username=username)


def load_comment_verification_runs_df(username, job_ids=None, limit=None):
    query = """
    SELECT *
    FROM comment_verification_runs
    WHERE dashboard_user = ?
    """
    params = [str(username)]
    if job_ids:
        normalized_job_ids = [int(item) for item in job_ids if str(item).strip()]
        if normalized_job_ids:
            placeholders = ",".join(["?"] * len(normalized_job_ids))
            query += f" AND comment_job_id IN ({placeholders})"
            params.extend(normalized_job_ids)
    query += " ORDER BY datetime(COALESCE(verified_at, created_at)) DESC, id DESC"
    if limit:
        query += f" LIMIT {int(limit)}"
    return _read_task_records(query, params=params)


def load_comment_verification_runs(username, job_ids=None, limit=None):
    df = load_comment_verification_runs_df(username, job_ids=job_ids, limit=limit)
    return df.to_dict("records") if not df.empty else []


def create_comment_verification_run(username, job, verifier_account):
    now = _now_str()
    conn = get_tasks_connection()
    try:
        cursor = conn.execute(
            """
            INSERT INTO comment_verification_runs (
                dashboard_user,
                comment_job_id,
                verifier_account_id,
                verifier_account_name,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(username),
                int(job["id"]),
                _normalize_mcp_account_id((verifier_account or {}).get("account_id") or ""),
                str((verifier_account or {}).get("account_name") or "").strip(),
                "running",
                now,
                now,
            ),
        )
        run_id = int(cursor.lastrowid)
        conn.commit()
    finally:
        conn.close()
    return run_id


def update_comment_verification_run(run_id, field_values):
    updates = {str(k): v for k, v in (field_values or {}).items() if str(k).strip()}
    if not updates:
        return
    updates["updated_at"] = _now_str()
    assignments = ", ".join([f"{key} = ?" for key in updates.keys()])
    params = list(updates.values()) + [int(run_id)]
    conn = get_tasks_connection()
    try:
        conn.execute(f"UPDATE comment_verification_runs SET {assignments} WHERE id = ?", params)
        conn.commit()
    finally:
        conn.close()


def build_comment_job_data(note, selected_template, selected_pkg, selected_account, comment_content, current_mode="note", selected_target=None, verifier_account=None, schedule_result=None, scheduled_for=None, force_status="queued"):
    verifier_account = verifier_account or {}
    schedule_result = schedule_result or {}
    strategy_suggested_for = schedule_result.get("scheduled_dt")
    if hasattr(strategy_suggested_for, "strftime"):
        strategy_suggested_for_text = strategy_suggested_for.strftime("%Y-%m-%d %H:%M:%S")
    else:
        strategy_suggested_for_text = str(strategy_suggested_for or "")
    scheduled_for_text = str(scheduled_for or strategy_suggested_for_text or _now_str())
    target = dict(selected_target or {})
    payload_snapshot = {
        "note_id": str(note.get("note_id") or ""),
        "note_title": str(note.get("title") or ""),
        "xsec_token": _get_note_xsec_token(note),
        "template_id": str(selected_template.get("id") or ""),
        "template_name": str(selected_template.get("name") or ""),
        "strategy_package_id": str(selected_pkg.get("id") or ""),
        "strategy_package_name": str(selected_pkg.get("name") or ""),
        "comment_content": str(comment_content or "").strip(),
        "target_type": str(current_mode or "note"),
        "target_comment_id": str(target.get("comment_id") or ""),
        "target_user_id": str(target.get("user_id") or ""),
        "target_nickname": str(target.get("nickname") or ""),
        "target_comment_preview": str(target.get("preview") or ""),
    }
    return {
        "note_id": str(note.get("note_id") or ""),
        "note_title": str(note.get("title") or ""),
        "note_url": str(note.get("note_url") or ""),
        "xsec_token": _get_note_xsec_token(note),
        "target_type": str(current_mode or "note"),
        "target_comment_id": str(target.get("comment_id") or ""),
        "target_user_id": str(target.get("user_id") or ""),
        "target_nickname": str(target.get("nickname") or ""),
        "target_comment_preview": str(target.get("preview") or ""),
        "template_id": str(selected_template.get("id") or ""),
        "template_name": str(selected_template.get("name") or ""),
        "strategy_package_id": str(selected_pkg.get("id") or ""),
        "strategy_package_name": str(selected_pkg.get("name") or ""),
        "comment_account_id": str(selected_account.get("account_id") or MCP_DEFAULT_RUNTIME_ACCOUNT_ID),
        "comment_account_name": str(selected_account.get("account_name") or ""),
        "verifier_account_id": _normalize_mcp_account_id(verifier_account.get("account_id") or ""),
        "verifier_account_name": str(verifier_account.get("account_name") or ""),
        "comment_content": str(comment_content or "").strip(),
        "status": str(force_status or "queued"),
        "scheduled_for": scheduled_for_text,
        "strategy_suggested_for": strategy_suggested_for_text or scheduled_for_text,
        "strategy_delayed": bool(schedule_result.get("delayed")),
        "jitter_sec": _coerce_non_negative_int(schedule_result.get("jitter_sec", 0)),
        "payload": payload_snapshot,
    }


def _increment_comment_template_usage(username, template_id):
    template_id = str(template_id or "").strip()
    if not template_id:
        return
    templates = load_comment_templates(username)
    changed = False
    for item in templates:
        if str(item.get("id")) == template_id:
            item["used_count"] = int(item.get("used_count", 0)) + 1
            item["last_used_at"] = _now_str()
            item["updated_at"] = _now_str()
            changed = True
            break
    if changed:
        save_comment_templates_for_user(username, templates)


def _build_legacy_comment_event_from_job(job, status, result_message="", error_message=""):
    return {
        "id": int(time.time() * 1000),
        "dashboard_user": str(job.get("dashboard_user") or st.session_state.username),
        "note_id": str(job.get("note_id") or ""),
        "note_title": str(job.get("note_title") or ""),
        "template_id": str(job.get("template_id") or ""),
        "template_name": str(job.get("template_name") or ""),
        "strategy_package_id": str(job.get("strategy_package_id") or ""),
        "strategy_package_name": str(job.get("strategy_package_name") or ""),
        "comment_account_id": str(job.get("comment_account_id") or ""),
        "comment_account_name": str(job.get("comment_account_name") or ""),
        "scheduled_for": str(job.get("scheduled_for") or job.get("created_at") or _now_str()),
        "strategy_suggested_for": str(job.get("strategy_suggested_for") or job.get("scheduled_for") or ""),
        "strategy_delayed": bool(job.get("strategy_delayed")),
        "jitter_sec": _coerce_non_negative_int(job.get("jitter_sec", 0)),
        "target_type": str(job.get("target_type") or "note"),
        "target_comment_id": str(job.get("target_comment_id") or ""),
        "target_user_id": str(job.get("target_user_id") or ""),
        "target_nickname": str(job.get("target_nickname") or ""),
        "target_comment_preview": str(job.get("target_comment_preview") or ""),
        "created_at": str(job.get("created_at") or _now_str()),
        "executed_at": str(job.get("executed_at") or _now_str()),
        "third_party_request_id": str(job.get("request_id") or "-"),
        "status": str(status or ""),
        "result_message": str(result_message or ""),
        "error_message": str(error_message or ""),
        "mcp_username": str(job.get("mcp_username") or ""),
    }


def append_legacy_comment_event_from_job(job, status, result_message="", error_message=""):
    username = str(job.get("dashboard_user") or st.session_state.username)
    all_events = load_comment_strategy_events(username)
    all_events.append(
        _build_legacy_comment_event_from_job(
            job,
            status=status,
            result_message=result_message,
            error_message=error_message,
        )
    )
    save_comment_strategy_events_for_user(username, all_events)


def _build_comment_api_payload_from_job(job):
    payload = _safe_json_loads(job.get("payload_json"), fallback={})
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("note_id", str(job.get("note_id") or ""))
    payload.setdefault("note_title", str(job.get("note_title") or ""))
    payload.setdefault("xsec_token", str(job.get("xsec_token") or ""))
    payload.setdefault("comment_content", str(job.get("comment_content") or ""))
    payload.setdefault("target_type", str(job.get("target_type") or "note"))
    payload.setdefault("target_comment_id", str(job.get("target_comment_id") or ""))
    payload.setdefault("target_user_id", str(job.get("target_user_id") or ""))
    payload.setdefault("target_nickname", str(job.get("target_nickname") or ""))
    payload.setdefault("target_comment_preview", str(job.get("target_comment_preview") or ""))
    return payload


def finalize_comment_job_send_success(job, api_result):
    now = _now_str()
    next_status = "verify_pending" if str(job.get("verifier_account_id") or "").strip() else "sent_api_ok"
    update_comment_job_fields(
        job["id"],
        {
            "status": next_status,
            "send_attempt_count": _coerce_non_negative_int(job.get("send_attempt_count", 0)) + 1,
            "request_id": str(api_result.get("request_id") or ""),
            "mcp_username": str(api_result.get("mcp_username") or ""),
            "posted_comment_id": str(api_result.get("comment_id") or ""),
            "result_message": str(api_result.get("message") or ""),
            "send_response_json": _json_dumps_compact(api_result),
            "last_error": None,
            "executed_at": now,
            "verification_reason": "" if next_status == "verify_pending" else "未配置验证账号，可后续手动回溯。",
        },
    )
    _increment_comment_template_usage(str(job.get("dashboard_user") or st.session_state.username), job.get("template_id"))
    update_note_status(str(job.get("note_id") or ""), "Commented")
    refreshed_job = get_comment_job(job["id"], username=str(job.get("dashboard_user") or st.session_state.username))
    if refreshed_job:
        append_legacy_comment_event_from_job(
            refreshed_job,
            status="sent",
            result_message=str(api_result.get("message") or ""),
        )
    return refreshed_job


def finalize_comment_job_send_failure(job, error_message):
    username = str(job.get("dashboard_user") or st.session_state.username)
    update_comment_job_fields(
        job["id"],
        {
            "status": "send_failed",
            "send_attempt_count": _coerce_non_negative_int(job.get("send_attempt_count", 0)) + 1,
            "last_error": str(error_message or ""),
            "result_message": "",
            "verification_reason": "",
        },
    )
    refreshed_job = get_comment_job(job["id"], username=str(job.get("dashboard_user") or st.session_state.username))
    if refreshed_job:
        append_legacy_comment_event_from_job(
            refreshed_job,
            status="failed",
            error_message=str(error_message or ""),
        )
    append_operation_exception(
        username=username,
        source_module="comment_send",
        severity="critical",
        error_message_raw=str(error_message or ""),
        error_message_normalized=f"评论发送失败：{error_message}",
        job_id=int(job.get("id")),
        note_id=str(job.get("note_id") or ""),
        account_id=str(job.get("comment_account_id") or ""),
        strategy_package_id=str(job.get("strategy_package_id") or ""),
    )
    return refreshed_job


def execute_claimed_comment_job(job):
    try:
        api_result = trigger_third_party_comment_api(
            account_id=str(job.get("comment_account_id") or ""),
            payload=_build_comment_api_payload_from_job(job),
        )
    except Exception as exc:
        refreshed_job = finalize_comment_job_send_failure(job, str(exc))
        return {
            "ok": False,
            "job": refreshed_job or job,
            "error": str(exc),
        }
    refreshed_job = finalize_comment_job_send_success(job, api_result)
    return {
        "ok": True,
        "job": refreshed_job or job,
        "result": api_result,
    }


def process_due_comment_jobs(username, limit=1, specific_job_ids=None):
    normalized_specific_ids = [int(item) for item in (specific_job_ids or []) if str(item).strip()]
    results = []
    if normalized_specific_ids:
        for job_id in normalized_specific_ids[: max(1, int(limit or len(normalized_specific_ids)) )]:
            claimed_job = claim_next_due_comment_job(username, specific_job_id=job_id)
            if not claimed_job:
                continue
            results.append(execute_claimed_comment_job(claimed_job))
        return results

    max_count = max(1, int(limit or 1))
    for _ in range(max_count):
        claimed_job = claim_next_due_comment_job(username)
        if not claimed_job:
            break
        results.append(execute_claimed_comment_job(claimed_job))
    return results


def _flatten_comment_candidates(raw_comments, parent_comment_id=""):
    flattened = []
    if not isinstance(raw_comments, list):
        return flattened
    for item in raw_comments:
        if not isinstance(item, dict):
            continue
        current_id = str(item.get("id") or item.get("comment_id") or "").strip()
        content = str(item.get("content") or "").strip()
        flattened.append(
            {
                "comment_id": current_id,
                "parent_comment_id": str(parent_comment_id or "").strip(),
                "content": content,
                "create_time": item.get("createTime") or item.get("create_time"),
                "like_count": parse_metric_value(item.get("likeCount") or item.get("like_count") or item.get("liked_count") or 0),
                "nickname": str(((item.get("userInfo") or {}).get("nickname")) or item.get("nickname") or "").strip(),
            }
        )
        flattened.extend(
            _flatten_comment_candidates(
                item.get("subComments") or item.get("sub_comments") or [],
                parent_comment_id=current_id,
            )
        )
    return flattened


def _match_comment_job_visibility(job, comment_candidates):
    posted_comment_id = str(job.get("posted_comment_id") or "").strip()
    if posted_comment_id:
        matched = next(
            (item for item in comment_candidates if str(item.get("comment_id") or "").strip() == posted_comment_id),
            None,
        )
        if matched:
            return "visible", matched, "comment_id"

    target_text = _normalize_comment_match_text(job.get("comment_content") or "")
    if not target_text:
        return "manual_review", None, "empty_comment_content"

    exact_matches = [
        item for item in comment_candidates
        if _normalize_comment_match_text(item.get("content") or "") == target_text
    ]
    if str(job.get("target_type") or "") == "reply" and str(job.get("target_comment_id") or "").strip():
        parent_exact_matches = [
            item for item in exact_matches
            if str(item.get("parent_comment_id") or "").strip() == str(job.get("target_comment_id") or "").strip()
        ]
        if len(parent_exact_matches) == 1:
            return "visible", parent_exact_matches[0], "parent_exact_text"
        if len(parent_exact_matches) > 1:
            return "manual_review", parent_exact_matches[0], "multiple_parent_exact_text"

    if len(exact_matches) == 1:
        return "visible", exact_matches[0], "exact_text"
    if len(exact_matches) > 1:
        return "manual_review", exact_matches[0], "multiple_exact_text"

    partial_matches = [
        item for item in comment_candidates
        if target_text and (
            target_text in _normalize_comment_match_text(item.get("content") or "")
            or _normalize_comment_match_text(item.get("content") or "") in target_text
        )
    ]
    if len(partial_matches) == 1:
        return "manual_review", partial_matches[0], "partial_text"
    if len(partial_matches) > 1:
        return "manual_review", partial_matches[0], "multiple_partial_text"

    return "hidden", None, "not_found"


def run_comment_job_verification(username, job_id, verifier_account_id=""):
    job = get_comment_job(job_id, username=username)
    if not job:
        raise RuntimeError("评论任务不存在，无法执行回溯。")

    available_accounts = load_verifier_accounts(username, active_only=True)
    selected_account = None
    normalized_verifier_account_id = _normalize_mcp_account_id(verifier_account_id or job.get("verifier_account_id") or "")
    if normalized_verifier_account_id:
        selected_account = next(
            (item for item in available_accounts if str(item.get("account_id") or "") == normalized_verifier_account_id),
            None,
        )
    if selected_account is None:
        selected_account = get_default_verifier_account(username)
    if not selected_account:
        raise RuntimeError("当前未配置可用的回溯检查账号，请先到“执行中心 > 账号与登录”添加并登录该账号。")

    run_id = create_comment_verification_run(username, job, selected_account)
    now = _now_str()
    try:
        _check_mcp_comment_api_status(selected_account.get("account_id"))
        detail_data = _fetch_mcp_feed_detail(
            note_id=job.get("note_id"),
            xsec_token=job.get("xsec_token"),
            account_id=selected_account.get("account_id"),
            load_all_comments=True,
        )
        comment_candidates = _flatten_comment_candidates(((detail_data.get("comments") or {}).get("list") or []))
        verdict, matched_comment, matched_by = _match_comment_job_visibility(job, comment_candidates)
        update_comment_verification_run(
            run_id,
            {
                "status": "completed",
                "verdict": verdict,
                "matched_comment_id": str((matched_comment or {}).get("comment_id") or ""),
                "matched_comment_content": str((matched_comment or {}).get("content") or ""),
                "matched_comment_like_count": _coerce_non_negative_int((matched_comment or {}).get("like_count", 0)),
                "matched_comment_create_time": str((matched_comment or {}).get("create_time") or ""),
                "matched_by": matched_by,
                "reason": matched_by,
                "result_json": _json_dumps_compact(
                    {
                        "matched_by": matched_by,
                        "matched_comment": matched_comment or {},
                        "checked_comment_count": len(comment_candidates),
                    }
                ),
                "verified_at": now,
            },
        )
        update_comment_job_fields(
            job_id,
            {
                "status": {
                    "visible": "verified_visible",
                    "hidden": "verified_hidden",
                    "manual_review": "manual_review",
                }.get(verdict, "manual_review"),
                "verification_attempt_count": _coerce_non_negative_int(job.get("verification_attempt_count", 0)) + 1,
                "last_verified_at": now,
                "verifier_account_id": str(selected_account.get("account_id") or ""),
                "verifier_account_name": str(selected_account.get("account_name") or ""),
                "verified_comment_id": str((matched_comment or {}).get("comment_id") or ""),
                "verified_comment_content": str((matched_comment or {}).get("content") or ""),
                "verified_comment_like_count": _coerce_non_negative_int((matched_comment or {}).get("like_count", 0)),
                "verification_reason": matched_by,
            },
        )
        return {
            "job": get_comment_job(job_id, username=username),
            "run_id": run_id,
            "verdict": verdict,
            "matched_comment": matched_comment,
        }
    except Exception as exc:
        update_comment_verification_run(
            run_id,
            {
                "status": "failed",
                "verdict": "failed",
                "error_message": str(exc),
                "reason": str(exc),
                "verified_at": now,
            },
        )
        append_operation_exception(
            username=username,
            source_module="comment_verify",
            severity="warn",
            error_message_raw=str(exc),
            error_message_normalized=f"评论回溯失败：{exc}",
            job_id=int(job_id),
            note_id=str(job.get("note_id") or ""),
            account_id=str(selected_account.get("account_id") or ""),
            strategy_package_id=str(job.get("strategy_package_id") or ""),
        )
        update_comment_job_fields(
            job_id,
            {
                "verification_attempt_count": _coerce_non_negative_int(job.get("verification_attempt_count", 0)) + 1,
                "last_verified_at": now,
                "verification_reason": str(exc),
            },
        )
        raise


def run_comment_job_verification_batch(username, job_ids, verifier_account_id=""):
    normalized_ids = [int(item) for item in job_ids if str(item).strip()]
    results = []
    for job_id in normalized_ids:
        try:
            results.append(
                {
                    "ok": True,
                    **run_comment_job_verification(username, job_id, verifier_account_id=verifier_account_id),
                }
            )
        except Exception as exc:
            results.append(
                {
                    "ok": False,
                    "job": get_comment_job(job_id, username=username),
                    "error": str(exc),
                }
            )
    return results


def _runtime_account_cache_key(account_id):
    return _normalize_mcp_account_id(account_id) or MCP_DEFAULT_RUNTIME_ACCOUNT_ID


def _decode_qrcode_image(image_src):
    raw = str(image_src or "").strip()
    if not raw:
        return None
    if raw.startswith("data:image") and "," in raw:
        try:
            return base64.b64decode(raw.split(",", 1)[1])
        except Exception:
            return None
    return raw


def _format_ratio(numerator, denominator):
    try:
        numerator = float(numerator)
        denominator = float(denominator)
    except Exception:
        return "-"
    if denominator <= 0:
        return "-"
    return f"{(numerator / denominator) * 100:.1f}%"


def render_mcp_runtime_account_controls(
    account_id,
    account_label,
    key_prefix,
    *,
    show_runtime_id=True,
    current_account_prefix="当前操作账号",
    check_button_label="检查登录态",
    qrcode_button_label="获取扫码二维码",
    clear_button_label="清除登录态",
    status_prefix="登录状态",
    profile_prefix="当前账号主页",
    show_profile_red_id=True,
    login_success_message="登录态检查成功。",
    already_logged_in_message="该账号已处于登录态，无需再次扫码。",
    qrcode_success_message="二维码已刷新，请使用对应小红书账号扫码。",
    clear_success_message="运行时登录态已清除。",
    qrcode_timeout_prefix="二维码有效期",
):
    runtime_key = _runtime_account_cache_key(account_id)
    status_cache = st.session_state.setdefault("mcp_runtime_status_cache", {})
    profile_cache = st.session_state.setdefault("mcp_runtime_profile_cache", {})
    qrcode_cache = st.session_state.setdefault("mcp_runtime_qrcode_cache", {})

    normalized_runtime_id = _normalize_mcp_account_id(account_id) or MCP_DEFAULT_RUNTIME_ACCOUNT_ID
    if show_runtime_id:
        st.caption(f"{current_account_prefix}：{account_label}（运行时ID：{normalized_runtime_id}）")
    else:
        st.caption(f"{current_account_prefix}：{account_label}")
    c1, c2, c3 = st.columns([1, 1, 1])
    if c1.button(check_button_label, key=f"{key_prefix}_check_status", use_container_width=True):
        try:
            status_cache[runtime_key] = _check_mcp_comment_api_status(account_id)
            try:
                profile_cache[runtime_key] = _get_mcp_my_profile(account_id)
            except Exception:
                profile_cache.pop(runtime_key, None)
            st.success(login_success_message)
        except Exception as exc:
            status_cache[runtime_key] = {"is_logged_in": False, "error": str(exc)}
            profile_cache.pop(runtime_key, None)
            st.warning(str(exc))
    if c2.button(qrcode_button_label, key=f"{key_prefix}_fetch_qrcode", use_container_width=True):
        try:
            qrcode_cache[runtime_key] = _get_mcp_login_qrcode(account_id)
            if qrcode_cache[runtime_key].get("is_logged_in"):
                status_cache[runtime_key] = {"is_logged_in": True, "account_id": runtime_key}
                try:
                    profile_cache[runtime_key] = _get_mcp_my_profile(account_id)
                except Exception:
                    profile_cache.pop(runtime_key, None)
                st.success(already_logged_in_message)
            else:
                st.success(qrcode_success_message)
        except Exception as exc:
            st.error(f"获取二维码失败：{exc}")
    if c3.button(clear_button_label, key=f"{key_prefix}_clear_login", use_container_width=True):
        try:
            _delete_mcp_login_cookies(account_id)
            status_cache.pop(runtime_key, None)
            profile_cache.pop(runtime_key, None)
            qrcode_cache.pop(runtime_key, None)
            st.success(clear_success_message)
        except Exception as exc:
            st.error(f"清除登录态失败：{exc}")

    status_data = status_cache.get(runtime_key) or {}
    profile_data = profile_cache.get(runtime_key) or {}
    qrcode_data = qrcode_cache.get(runtime_key) or {}

    status_text = "已登录" if status_data.get("is_logged_in") else "未登录 / 未检查"
    st.caption(f"{status_prefix}：{status_text}")
    if isinstance(profile_data, dict) and profile_data:
        user_basic_info = profile_data.get("userBasicInfo") or {}
        profile_nickname = str(user_basic_info.get("nickname") or "").strip()
        profile_red_id = str(user_basic_info.get("redId") or "").strip()
        if profile_nickname or profile_red_id:
            if show_profile_red_id and profile_red_id:
                st.caption(f"{profile_prefix}：{profile_nickname or '-'}｜RedId: {profile_red_id}")
            elif profile_nickname:
                st.caption(f"{profile_prefix}：{profile_nickname}")
            elif profile_red_id:
                st.caption(f"{profile_prefix}：已识别该账号")
    image_bytes = _decode_qrcode_image(qrcode_data.get("img"))
    if image_bytes:
        st.image(image_bytes, width=220)
        if qrcode_data.get("timeout"):
            st.caption(f"{qrcode_timeout_prefix}：{qrcode_data.get('timeout')}")


def _is_comment_execution_advanced_view():
    return str(st.session_state.get("comment_execution_view_mode", "简洁视图")) == "高级视图"


def _friendly_comment_executor_account_name(account_name="", account_id="", include_id=False):
    normalized_name = str(account_name or "").strip()
    normalized_id = str(account_id or "").strip()
    if normalized_name and normalized_name != normalized_id:
        base_label = normalized_name
    elif normalized_id == MCP_DEFAULT_RUNTIME_ACCOUNT_ID or not normalized_id:
        base_label = "系统默认发送账号"
    else:
        base_label = "未命名账号"
    if include_id and normalized_id:
        return f"{base_label}（ID: {normalized_id}）"
    return base_label


def _friendly_verifier_account_name(account_name="", account_id="", include_id=False):
    normalized_name = str(account_name or "").strip()
    normalized_id = str(account_id or "").strip()
    if normalized_name and normalized_name != normalized_id:
        base_label = normalized_name
    else:
        base_label = "未命名账号"
    if include_id and normalized_id:
        return f"{base_label}（ID: {normalized_id}）"
    return base_label


def _format_comment_job_selector_label(item):
    if not isinstance(item, dict):
        return "-"
    note_title = _summarize_text(item.get("note_title") or "未命名笔记", 20)
    scheduled_for = str(item.get("scheduled_for") or "-")
    status_label = format_comment_job_status(item.get("status"))
    return f"{note_title}｜{scheduled_for}｜{status_label}"


def _format_exception_selector_label(item):
    if not isinstance(item, dict):
        return "-"
    occurred_at = str(item.get("occurred_at") or "-")
    return (
        f"{_operation_exception_source_label(item.get('source_module'))}"
        f"｜{_operation_exception_severity_label(item.get('severity'))}"
        f"｜{occurred_at}"
    )


def _format_comment_job_verification_outcome(status):
    normalized_status = str(status or "").strip()
    mapping = {
        "verified_visible": "已确认上评",
        "verified_hidden": "未确认上评",
        "manual_review": "需人工复核",
        "verify_pending": "待检查",
        "sent_api_ok": "待检查",
    }
    return mapping.get(normalized_status, format_comment_job_status(normalized_status))


def _format_verification_run_conclusion(item):
    if not isinstance(item, dict):
        return "-"
    verdict = str(item.get("verdict") or "").strip()
    if verdict:
        return format_comment_verdict(verdict)
    status = str(item.get("status") or "").strip()
    if status == "failed":
        return "检查失败"
    if status == "pending":
        return "检查中"
    return "-"


@st.fragment(run_every="5s")
def render_comment_execution_queue_fragment(username):
    is_advanced_view = _is_comment_execution_advanced_view()
    jobs_df = load_comment_jobs_df(username, limit=200)
    if jobs_df.empty:
        st.info("暂无评论队列任务。")
        return

    jobs = jobs_df.to_dict("records")
    now_dt = datetime.now()
    due_count = 0
    future_count = 0
    for item in jobs:
        if str(item.get("status") or "") != "queued":
            continue
        scheduled_dt = _to_dt(item.get("scheduled_for"))
        if scheduled_dt and scheduled_dt <= now_dt:
            due_count += 1
        else:
            future_count += 1

    dispatching_count = sum(1 for item in jobs if str(item.get("status") or "") == "dispatching")
    verify_pending_count = sum(
        1 for item in jobs
        if str(item.get("status") or "") in {"sent_api_ok", "verify_pending"}
    )
    send_failed_count = sum(1 for item in jobs if str(item.get("status") or "") == "send_failed")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("到期待执行", due_count)
    m2.metric("发送中 / 待回溯", dispatching_count + verify_pending_count)
    m3.metric("发送失败", send_failed_count)
    m4.metric("未到期排队", future_count)
    st.caption("默认只看结果；批量排程、自动执行和完整技术字段已收进高级区域。")

    status_options = list(COMMENT_JOB_STATUS_LABELS.keys())
    selected_statuses = st.multiselect(
        "当前状态筛选",
        options=status_options,
        default=[
            "queued",
            "dispatching",
            "sent_api_ok",
            "send_failed",
            "verify_pending",
        ],
        format_func=format_comment_job_status,
        key="comment_execution_status_filters",
    )
    filtered_jobs = [
        item for item in jobs
        if not selected_statuses or str(item.get("status") or "") in selected_statuses
    ]
    action_col, helper_col = st.columns([1.2, 3])
    if action_col.button("执行到期任务", key="comment_execution_run_due", use_container_width=True):
        results = process_due_comment_jobs(username, limit=10)
        if not results:
            st.info("当前没有到期任务可执行。")
        else:
            ok_count = sum(1 for item in results if item.get("ok"))
            fail_count = sum(1 for item in results if not item.get("ok"))
            st.success(f"已执行 {len(results)} 条任务，其中成功 {ok_count} 条，失败 {fail_count} 条。")
            st.rerun()
    with helper_col:
        if is_advanced_view:
            st.caption("当前为高级视图，可展开下方区域查看完整字段和批量执行控制。")
        else:
            st.caption("如需立即执行、重试、取消或自动轮询，请展开“高级执行控制”。")

    simple_rows = []
    advanced_rows = []
    job_lookup = {}
    for item in filtered_jobs:
        job_id = int(item.get("id"))
        job_lookup[job_id] = item
        simple_rows.append(
            {
                "计划执行时间": item.get("scheduled_for") or "-",
                "笔记": _summarize_text(item.get("note_title"), 28),
                "评论内容": _summarize_text(item.get("comment_content"), 36),
                "发送账号": _friendly_comment_executor_account_name(
                    item.get("comment_account_name"),
                    item.get("comment_account_id"),
                ),
                "当前状态": format_comment_job_status(item.get("status")),
                "结果说明": item.get("last_error") or item.get("verification_reason") or item.get("result_message") or "-",
            }
        )
        advanced_rows.append(
            {
                "任务ID": job_id,
                "计划执行时间": item.get("scheduled_for") or "-",
                "笔记": _summarize_text(item.get("note_title"), 28),
                "评论方式": "回复评论" if str(item.get("target_type") or "") == "reply" else "整帖评论",
                "评论内容": _summarize_text(item.get("comment_content"), 36),
                "发送账号": _friendly_comment_executor_account_name(
                    item.get("comment_account_name"),
                    item.get("comment_account_id"),
                    include_id=is_advanced_view,
                ),
                "回溯检查账号": _friendly_verifier_account_name(
                    item.get("verifier_account_name"),
                    item.get("verifier_account_id"),
                    include_id=True,
                ),
                "当前状态": format_comment_job_status(item.get("status")),
                "结果说明": item.get("last_error") or item.get("verification_reason") or item.get("result_message") or "-",
                "实际执行时间": item.get("executed_at") or "-",
            }
        )

    if simple_rows:
        st.dataframe(pd.DataFrame(simple_rows), use_container_width=True, hide_index=True)
    else:
        st.info("当前筛选条件下无任务。")

    selectable_job_ids = list(job_lookup.keys())
    with st.expander("高级执行控制", expanded=is_advanced_view):
        st.caption("这里保留自动执行、立即执行、重试和取消等运维操作，默认对结果视角收起。")
        selected_job_ids = st.multiselect(
            "选择要操作的评论任务",
            options=selectable_job_ids,
            default=[
                item for item in st.session_state.get("comment_execution_selected_job_ids", [])
                if item in selectable_job_ids
            ],
            format_func=lambda job_id: _format_comment_job_selector_label(job_lookup.get(job_id)),
            key="comment_execution_selected_job_ids",
        )
        auto_execute = st.checkbox(
            "页面打开时，每 5 秒自动执行 1 条到期任务",
            key="comment_execution_auto_run",
        )
        if auto_execute and not is_advanced_view:
            st.caption("为避免误触发，自动执行仅在“高级视图”下生效。")
        elif auto_execute and due_count > 0:
            auto_results = process_due_comment_jobs(username, limit=1)
            if auto_results:
                ok_count = sum(1 for item in auto_results if item.get("ok"))
                fail_count = sum(1 for item in auto_results if not item.get("ok"))
                st.caption(f"自动执行结果：成功 {ok_count} 条，失败 {fail_count} 条。")
        elif auto_execute:
            st.caption("当前没有到期任务，自动执行未触发。")

        a1, a2, a3 = st.columns([1, 1, 1])
        if a1.button("立即执行所选", key="comment_execution_run_selected", use_container_width=True, disabled=not selected_job_ids):
            schedule_comment_jobs_now(username, selected_job_ids)
            results = process_due_comment_jobs(username, limit=len(selected_job_ids), specific_job_ids=selected_job_ids)
            ok_count = sum(1 for item in results if item.get("ok"))
            fail_count = sum(1 for item in results if not item.get("ok"))
            st.success(f"所选任务已触发执行：成功 {ok_count} 条，失败 {fail_count} 条。")
            st.rerun()
        if a2.button("重试所选", key="comment_execution_retry_selected", use_container_width=True, disabled=not selected_job_ids):
            changed = requeue_comment_jobs(username, selected_job_ids)
            st.success(f"已重排 {changed} 条失败/取消任务。")
            st.rerun()
        if a3.button("取消所选", key="comment_execution_cancel_selected", use_container_width=True, disabled=not selected_job_ids):
            changed = cancel_comment_jobs(username, selected_job_ids)
            st.success(f"已取消 {changed} 条任务。")
            st.rerun()

    with st.expander("高级信息", expanded=is_advanced_view):
        st.caption("这里保留任务 ID、回溯检查账号、评论方式、实际执行时间等排障字段。")
        if advanced_rows:
            st.dataframe(pd.DataFrame(advanced_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("当前筛选条件下暂无任务明细。")


@st.fragment(run_every="5s")
def render_comment_execution_verification_fragment(username):
    is_advanced_view = _is_comment_execution_advanced_view()
    verifier_accounts = load_verifier_accounts(username, active_only=True)
    verifier_options = [str(item.get("account_id")) for item in verifier_accounts]
    st.caption("回溯检查账号用于从旁观视角确认评论是否真的外部可见。")
    selected_verifier_id = st.selectbox(
        "回溯检查账号",
        options=verifier_options if verifier_options else ["__none__"],
        index=0,
        format_func=lambda x: (
            "暂无可用回溯检查账号"
            if x == "__none__"
            else next(
                (
                    _friendly_verifier_account_name(
                        item.get("account_name"),
                        item.get("account_id"),
                        include_id=is_advanced_view,
                    )
                    for item in verifier_accounts if str(item.get("account_id")) == str(x)
                ),
                str(x),
            )
        ),
        key="comment_execution_verify_account_selected",
    )

    jobs_df = load_comment_jobs_df(username, statuses=list(COMMENT_JOB_VERIFICATION_READY_STATUSES), limit=200)
    if jobs_df.empty:
        st.info("暂无待检查或已检查的评论任务。")
        return
    jobs = jobs_df.to_dict("records")

    status_options = ["verify_pending", "verified_hidden", "manual_review", "verified_visible", "sent_api_ok"]
    selected_statuses = st.multiselect(
        "当前状态筛选",
        options=status_options,
        default=status_options,
        format_func=format_comment_job_status,
        key="comment_verification_status_filters",
    )
    filtered_jobs = [
        item for item in jobs
        if not selected_statuses or str(item.get("status") or "") in selected_statuses
    ]
    job_lookup = {int(item.get("id")): item for item in filtered_jobs}
    selectable_job_ids = list(job_lookup.keys())
    selected_job_ids = st.multiselect(
        "选择要检查的评论（可选）",
        options=selectable_job_ids,
        default=[
            item for item in st.session_state.get("comment_verify_selected_job_ids", [])
            if item in selectable_job_ids
        ],
        format_func=lambda job_id: _format_comment_job_selector_label(job_lookup.get(job_id)),
        key="comment_verify_selected_job_ids",
    )

    b1, b2 = st.columns([1, 1])
    if b1.button("检查未验收评论", key="comment_verify_pending_jobs", use_container_width=True, disabled=selected_verifier_id == "__none__"):
        pending_ids = [
            int(item.get("id"))
            for item in filtered_jobs
            if str(item.get("status") or "") in {"verify_pending", "sent_api_ok"}
        ]
        if not pending_ids:
            st.info("当前没有待检查的评论。")
        else:
            results = run_comment_job_verification_batch(username, pending_ids[:20], verifier_account_id=selected_verifier_id)
            ok_count = sum(1 for item in results if item.get("ok"))
            fail_count = sum(1 for item in results if not item.get("ok"))
            st.success(f"检查完成：成功 {ok_count} 条，失败 {fail_count} 条。")
            st.rerun()
    if b2.button("检查所选评论", key="comment_verify_selected_jobs", use_container_width=True, disabled=(selected_verifier_id == "__none__" or not selected_job_ids)):
        results = run_comment_job_verification_batch(username, selected_job_ids, verifier_account_id=selected_verifier_id)
        ok_count = sum(1 for item in results if item.get("ok"))
        fail_count = sum(1 for item in results if not item.get("ok"))
        st.success(f"所选评论检查完成：成功 {ok_count} 条，失败 {fail_count} 条。")
        st.rerun()

    if selected_verifier_id == "__none__":
        st.warning("请先在“账号与登录”中新增并登录回溯检查账号，然后再执行检查。")

    st.caption("接口发送成功不等于外部已可见，仍需回溯确认。")
    simple_rows = []
    advanced_rows = []
    for item in filtered_jobs:
        simple_rows.append(
            {
                "笔记": _summarize_text(item.get("note_title"), 28),
                "评论内容": _summarize_text(item.get("comment_content"), 34),
                "当前状态": format_comment_job_status(item.get("status")),
                "回溯结论": _format_comment_job_verification_outcome(item.get("status")),
                "最近回溯时间": item.get("last_verified_at") or "-",
                "判定说明": item.get("verification_reason") or item.get("result_message") or item.get("last_error") or "-",
            }
        )
        advanced_rows.append(
            {
                "任务ID": int(item.get("id")),
                "笔记": _summarize_text(item.get("note_title"), 28),
                "评论内容": _summarize_text(item.get("comment_content"), 34),
                "当前状态": format_comment_job_status(item.get("status")),
                "回溯结论": _format_comment_job_verification_outcome(item.get("status")),
                "回溯检查账号": _friendly_verifier_account_name(
                    item.get("verifier_account_name"),
                    item.get("verifier_account_id"),
                    include_id=True,
                ),
                "命中评论ID": item.get("verified_comment_id") or item.get("posted_comment_id") or "-",
                "命中评论点赞": _coerce_non_negative_int(item.get("verified_comment_like_count", 0)),
                "最近回溯时间": item.get("last_verified_at") or "-",
                "判定说明": item.get("verification_reason") or item.get("result_message") or item.get("last_error") or "-",
            }
        )

    if simple_rows:
        st.dataframe(pd.DataFrame(simple_rows), use_container_width=True, hide_index=True)
    else:
        st.info("当前筛选条件下暂无评论记录。")

    with st.expander("最近回溯流水", expanded=is_advanced_view):
        runs_df = load_comment_verification_runs_df(username, limit=50)
        if runs_df.empty:
            st.caption("暂无回溯流水。")
        else:
            run_rows = []
            for item in runs_df.to_dict("records"):
                if is_advanced_view:
                    run_rows.append(
                        {
                            "流水ID": int(item.get("id")),
                            "任务ID": int(item.get("comment_job_id")),
                            "回溯检查账号": _friendly_verifier_account_name(
                                item.get("verifier_account_name"),
                                item.get("verifier_account_id"),
                                include_id=True,
                            ),
                            "执行状态": str(item.get("status") or "-"),
                            "结论": _format_verification_run_conclusion(item),
                            "命中方式": item.get("matched_by") or "-",
                            "命中评论ID": item.get("matched_comment_id") or "-",
                            "命中评论点赞": _coerce_non_negative_int(item.get("matched_comment_like_count", 0)),
                            "结论说明": item.get("reason") or item.get("error_message") or "-",
                            "完成时间": item.get("verified_at") or item.get("created_at") or "-",
                        }
                    )
                else:
                    run_rows.append(
                        {
                            "完成时间": item.get("verified_at") or item.get("created_at") or "-",
                            "结论": _format_verification_run_conclusion(item),
                            "结论说明": item.get("reason") or item.get("error_message") or "-",
                        }
                    )
            st.dataframe(pd.DataFrame(run_rows), use_container_width=True, hide_index=True)

    with st.expander("高级信息", expanded=is_advanced_view):
        st.caption("这里保留任务 ID、回溯检查账号、命中评论信息等完整排障字段。")
        if advanced_rows:
            st.dataframe(pd.DataFrame(advanced_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("当前筛选条件下暂无任务明细。")


@st.fragment(run_every="5s")
def render_comment_execution_metrics_fragment(username):
    is_advanced_view = _is_comment_execution_advanced_view()
    jobs_df = load_comment_jobs_df(username, limit=2000)
    if jobs_df.empty:
        st.info("暂无评论执行数据。")
        return

    jobs_df = jobs_df.copy()
    jobs_df["status"] = jobs_df["status"].fillna("").astype(str)
    jobs_df["account_id_display"] = jobs_df["comment_account_id"].fillna("").astype(str).replace({"": MCP_DEFAULT_RUNTIME_ACCOUNT_ID})
    jobs_df["account_name_display"] = jobs_df["comment_account_name"].fillna("").astype(str)
    jobs_df.loc[jobs_df["account_name_display"] == "", "account_name_display"] = jobs_df["account_id_display"]

    send_success_count = int(jobs_df["status"].isin(list(COMMENT_JOB_SUCCESS_STATUSES)).sum())
    send_failed_count = int((jobs_df["status"] == "send_failed").sum())
    verify_pending_count = int((jobs_df["status"] == "verify_pending").sum())
    visible_count = int((jobs_df["status"] == "verified_visible").sum())
    hidden_count = int((jobs_df["status"] == "verified_hidden").sum())

    p1, p2, p3, p4 = st.columns(4)
    p1.metric("累计任务数", len(jobs_df))
    p2.metric("接口成功率", _format_ratio(send_success_count, send_success_count + send_failed_count))
    p3.metric("待回溯数量", verify_pending_count)
    p4.metric("已确认上评", visible_count)
    st.caption("接口成功率 = 技术发送成功比例；上评成功率 = 已完成回溯任务中真正可见的比例；端到端成功率 = 从发送到最终可见的整体成功率。")
    st.caption("接口发送成功不等于外部已可见，仍需回溯确认。")

    simple_metric_rows = []
    advanced_metric_rows = []
    for _, group in jobs_df.groupby(["account_id_display", "account_name_display"], dropna=False):
        row = group.iloc[0]
        send_success = int(group["status"].isin(list(COMMENT_JOB_SUCCESS_STATUSES)).sum())
        send_failed = int((group["status"] == "send_failed").sum())
        visible = int((group["status"] == "verified_visible").sum())
        hidden = int((group["status"] == "verified_hidden").sum())
        manual_review = int((group["status"] == "manual_review").sum())
        completed_verifications = visible + hidden + manual_review
        display_name = _friendly_comment_executor_account_name(
            row["account_name_display"],
            row["account_id_display"],
        )
        simple_metric_rows.append(
            {
                "发送账号": display_name,
                "累计任务": int(len(group)),
                "接口成功率": _format_ratio(send_success, send_success + send_failed),
                "上评成功率": _format_ratio(visible, completed_verifications),
                "端到端成功率": _format_ratio(visible, send_success + send_failed),
            }
        )
        advanced_metric_rows.append(
            {
                "发送账号": display_name,
                "账号ID": row["account_id_display"],
                "累计任务": int(len(group)),
                "接口发送成功": send_success,
                "接口发送失败": send_failed,
                "待回溯": int((group["status"] == "verify_pending").sum()),
                "确认上评": visible,
                "未确认上评": hidden,
                "人工复核": manual_review,
                "接口成功率": _format_ratio(send_success, send_success + send_failed),
                "上评成功率": _format_ratio(visible, completed_verifications),
                "端到端成功率": _format_ratio(visible, send_success + send_failed),
                "最近实际执行账号": str(row.get("mcp_username") or "-"),
            }
        )
    simple_metric_rows = sorted(simple_metric_rows, key=lambda item: item.get("累计任务", 0), reverse=True)
    advanced_metric_rows = sorted(advanced_metric_rows, key=lambda item: item.get("累计任务", 0), reverse=True)
    st.dataframe(pd.DataFrame(simple_metric_rows), use_container_width=True, hide_index=True)

    with st.expander("高级信息", expanded=is_advanced_view):
        st.caption("这里补充账号 ID、接口明细、回溯明细和最近实际执行账号，便于运营排查。")
        st.dataframe(pd.DataFrame(advanced_metric_rows), use_container_width=True, hide_index=True)

@st.fragment(run_every="5s")
def render_operation_exceptions_fragment(username):
    is_advanced_view = _is_comment_execution_advanced_view()
    rows = load_operation_exceptions(username)
    if not rows:
        st.info("暂无异常记录。")
        return

    total_count = len(rows)
    open_count = sum(1 for item in rows if str(item.get("status")) == "open")
    critical_open_count = sum(
        1
        for item in rows
        if str(item.get("status")) == "open" and str(item.get("severity")) == "critical"
    )
    resolved_count = sum(1 for item in rows if str(item.get("status")) == "resolved")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("异常总数", total_count)
    m2.metric("待处理", open_count)
    m3.metric("阻塞未处理", critical_open_count)
    m4.metric("已解决", resolved_count)

    source_options = sorted({str(item.get("source_module") or "") for item in rows if str(item.get("source_module") or "").strip()})
    severity_options = sorted({str(item.get("severity") or "") for item in rows if str(item.get("severity") or "").strip()})
    status_options = ["open", "resolved", "ignored"]
    employee_options = sorted({str(item.get("employee_name") or "") for item in rows if str(item.get("employee_name") or "").strip()})
    account_options = sorted({str(item.get("account_id") or "") for item in rows if str(item.get("account_id") or "").strip()})

    f1, f2, f3 = st.columns(3)
    selected_sources = f1.multiselect(
        "按来源筛选",
        options=source_options,
        default=source_options,
        format_func=_operation_exception_source_label,
        key="operation_exception_source_filters",
    )
    selected_severities = f2.multiselect(
        "按严重级别筛选",
        options=severity_options,
        default=severity_options,
        format_func=_operation_exception_severity_label,
        key="operation_exception_severity_filters",
    )
    selected_statuses = f3.multiselect(
        "按处理状态筛选",
        options=status_options,
        default=["open", "resolved", "ignored"],
        format_func=_operation_exception_status_label,
        key="operation_exception_status_filters",
    )
    with st.expander("高级筛选", expanded=is_advanced_view):
        f4, f5 = st.columns(2)
        selected_employees = f4.multiselect(
            "按员工筛选",
            options=employee_options,
            default=[],
            key="operation_exception_employee_filters",
        )
        selected_accounts = f5.multiselect(
            "按账号筛选",
            options=account_options,
            default=[],
            key="operation_exception_account_filters",
        )

    filtered_rows = []
    for item in rows:
        source = str(item.get("source_module") or "")
        severity = str(item.get("severity") or "")
        status = str(item.get("status") or "")
        employee_name = str(item.get("employee_name") or "")
        account_id = str(item.get("account_id") or "")
        if selected_sources and source not in selected_sources:
            continue
        if selected_severities and severity not in selected_severities:
            continue
        if selected_statuses and status not in selected_statuses:
            continue
        if selected_employees and employee_name not in selected_employees:
            continue
        if selected_accounts and account_id not in selected_accounts:
            continue
        filtered_rows.append(item)

    if not filtered_rows:
        st.info("当前筛选条件下无异常记录。")
        return

    selectable_ids = [str(item.get("exception_id", "")) for item in filtered_rows if str(item.get("exception_id", "")).strip()]
    exception_lookup = {
        str(item.get("exception_id", "")): item
        for item in filtered_rows
        if str(item.get("exception_id", "")).strip()
    }
    selected_exception_ids = st.multiselect(
        "选择要处理的异常",
        options=selectable_ids,
        default=[
            item for item in st.session_state.get("operation_exception_selected_ids", [])
            if item in selectable_ids
        ],
        format_func=lambda exception_id: _format_exception_selector_label(exception_lookup.get(exception_id)),
        key="operation_exception_selected_ids",
    )
    a1, a2, a3 = st.columns(3)
    if a1.button("标记已解决", use_container_width=True, disabled=not selected_exception_ids, key="operation_exception_mark_resolved"):
        changed = update_operation_exceptions_status(username, selected_exception_ids, "resolved")
        st.success(f"已更新 {changed} 条异常为已解决。")
        st.rerun()
    if a2.button("标记忽略", use_container_width=True, disabled=not selected_exception_ids, key="operation_exception_mark_ignored"):
        changed = update_operation_exceptions_status(username, selected_exception_ids, "ignored")
        st.success(f"已更新 {changed} 条异常为已忽略。")
        st.rerun()
    if a3.button("重新打开", use_container_width=True, disabled=not selected_exception_ids, key="operation_exception_mark_open"):
        changed = update_operation_exceptions_status(username, selected_exception_ids, "open")
        st.success(f"已更新 {changed} 条异常为待处理。")
        st.rerun()

    simple_rows = []
    advanced_rows = []
    for item in filtered_rows:
        simple_rows.append(
            {
                "发生时间": str(item.get("occurred_at") or "-"),
                "来源": _operation_exception_source_label(item.get("source_module")),
                "严重级别": _operation_exception_severity_label(item.get("severity")),
                "处理状态": _operation_exception_status_label(item.get("status")),
                "错误信息": str(item.get("error_message_normalized") or item.get("error_message_raw") or "-"),
                "建议": str(item.get("guidance") or "-"),
            }
        )
        advanced_rows.append(
            {
                "异常ID": str(item.get("exception_id") or "-"),
                "发生时间": str(item.get("occurred_at") or "-"),
                "来源": _operation_exception_source_label(item.get("source_module")),
                "严重级别": _operation_exception_severity_label(item.get("severity")),
                "处理状态": _operation_exception_status_label(item.get("status")),
                "员工": str(item.get("employee_name") or "-"),
                "账号": str(item.get("account_id") or "-"),
                "任务ID": item.get("task_id") or item.get("job_id") or "-",
                "错误信息": str(item.get("error_message_normalized") or item.get("error_message_raw") or "-"),
                "建议": str(item.get("guidance") or "-"),
            }
        )
    st.dataframe(pd.DataFrame(simple_rows), use_container_width=True, hide_index=True)

    with st.expander("高级信息", expanded=is_advanced_view):
        st.caption("这里补充异常 ID、员工、账号、任务 ID 等排障字段。")
        st.dataframe(pd.DataFrame(advanced_rows), use_container_width=True, hide_index=True)

def _get_agent_report_settings():
    runtime_api_key = ""
    runtime_base_url = ""
    runtime_model = ""
    runtime_username = ""
    try:
        runtime_username = str(st.session_state.get("username", "")).strip()
    except Exception:
        runtime_username = ""
    if runtime_username:
        runtime_settings = load_llm_runtime_settings_for_user(runtime_username)
        runtime_api_key = str(runtime_settings.get("api_key", "")).strip()
        runtime_base_url = str(runtime_settings.get("base_url", "")).strip()
        runtime_model = str(runtime_settings.get("model", "")).strip()

    api_key = (
        runtime_api_key
        or str(getattr(config, "AGENT_REPORT_API_KEY", "") or os.getenv("AGENT_REPORT_API_KEY", "")).strip()
        or str(getattr(config, "LIST_FILTER_API_KEY", "") or os.getenv("XHS_LIST_FILTER_API_KEY", "")).strip()
    )
    base_url = (
        runtime_base_url
        or str(os.getenv("XHS_LIST_FILTER_API_BASE", "")).strip()
        or str(getattr(config, "AGENT_REPORT_API_BASE", "") or os.getenv("AGENT_REPORT_API_BASE", "")).strip()
        or str(getattr(config, "LIST_FILTER_API_BASE", "https://api.openai.com/v1")).strip()
    ).rstrip("/")
    model = (
        runtime_model
        or str(os.getenv("XHS_LIST_FILTER_MODEL", "")).strip()
        or str(getattr(config, "AGENT_REPORT_MODEL", "") or os.getenv("AGENT_REPORT_MODEL", "")).strip()
        or str(getattr(config, "LIST_FILTER_MODEL", "gpt-4o-mini")).strip()
    )
    timeout_sec = _coerce_non_negative_int(
        getattr(config, "AGENT_REPORT_TIMEOUT_SEC", 30)
        or os.getenv("AGENT_REPORT_TIMEOUT_SEC", "30")
        or getattr(config, "LIST_FILTER_TIMEOUT_SEC", 20)
        or 20,
        default=30,
    ) or 30
    return {
        "api_key": api_key,
        "base_url": base_url,
        "model": model,
        "timeout_sec": timeout_sec,
    }

def _extract_json_payload(raw_content):
    content = str(raw_content or "").strip()
    if not content:
        raise ValueError("empty_content")
    if content.startswith("```"):
        content = content.strip("`")
        if content.startswith("json"):
            content = content[4:].strip()
    try:
        return json.loads(content)
    except Exception:
        object_match = re.search(r"(\{.*\})", content, flags=re.DOTALL)
        if object_match:
            return json.loads(object_match.group(1))
        array_match = re.search(r"(\[.*\])", content, flags=re.DOTALL)
        if array_match:
            return json.loads(array_match.group(1))
        raise

def _call_agent_report_llm(system_prompt, user_prompt, temperature=0.1):
    settings = _get_agent_report_settings()
    if not settings["api_key"]:
        raise RuntimeError("未配置 AGENT_REPORT_API_KEY 或 LIST_FILTER_API_KEY，无法生成老板看板内容。")

    payload = {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": str(system_prompt).strip()},
            {"role": "user", "content": str(user_prompt).strip()},
        ],
        "temperature": temperature,
    }
    headers = {
        "Authorization": f"Bearer {settings['api_key']}",
        "Content-Type": "application/json",
    }
    request = urllib.request.Request(
        f"{settings['base_url']}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=settings["timeout_sec"]) as response:
            raw = response.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"LLM 请求失败（HTTP {exc.code}）：{body[:500] or exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("无法连接到 LLM 服务，请检查 API 地址、网络和密钥配置。") from exc

    try:
        data = json.loads(raw)
    except Exception as exc:
        raise RuntimeError("LLM 服务返回了无法解析的 JSON 数据。") from exc

    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    usage = data.get("usage") or {}
    return {
        "content": content,
        "usage": {
            "prompt_tokens": _coerce_non_negative_int(usage.get("prompt_tokens", 0)),
            "completion_tokens": _coerce_non_negative_int(usage.get("completion_tokens", 0)),
            "total_tokens": _coerce_non_negative_int(usage.get("total_tokens", 0)),
        },
        "model": str(data.get("model") or settings["model"]),
    }

def get_employee_kpi_snapshots(username, report_date=None):
    conn = get_tasks_connection()
    try:
        if report_date:
            df = pd.read_sql_query(
                """
                SELECT * FROM daily_employee_kpi_snapshots
                WHERE dashboard_user = ? AND report_date = ?
                ORDER BY created_at DESC
                """,
                conn,
                params=(str(username), _report_date_to_str(report_date)),
            )
        else:
            df = pd.read_sql_query(
                """
                SELECT * FROM daily_employee_kpi_snapshots
                WHERE dashboard_user = ?
                ORDER BY report_date DESC, created_at DESC
                """,
                conn,
                params=(str(username),),
            )
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        return []
    records = df.to_dict("records")
    for item in records:
        item["target_crawl_count"] = _coerce_non_negative_int(item.get("target_crawl_count", 0))
        item["target_comment_count"] = _coerce_non_negative_int(item.get("target_comment_count", 0))
        item["prompt_tokens"] = _coerce_non_negative_int(item.get("prompt_tokens", 0))
        item["completion_tokens"] = _coerce_non_negative_int(item.get("completion_tokens", 0))
        item["total_tokens"] = _coerce_non_negative_int(item.get("total_tokens", 0))
        item["risk_level"] = _normalize_risk_level(item.get("risk_level"))
        item["source_snapshot"] = _safe_json_loads(item.get("source_snapshot_json"), {})
    return records

def get_employee_kpi_snapshot(username, employee_id, report_date):
    snapshot_map = {
        str(item.get("employee_id", "")): item
        for item in get_employee_kpi_snapshots(username, report_date)
    }
    return snapshot_map.get(str(employee_id))

def upsert_employee_kpi_snapshot(username, snapshot):
    now = _now_str()
    record = dict(snapshot or {})
    conn = get_tasks_connection()
    conn.execute(
        """
        INSERT INTO daily_employee_kpi_snapshots (
            dashboard_user,
            employee_id,
            employee_name_snapshot,
            employee_type_snapshot,
            goal_description_snapshot,
            report_date,
            target_crawl_count,
            target_comment_count,
            risk_level,
            reasoning,
            source_snapshot_json,
            prompt_id,
            prompt_updated_at,
            llm_model,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dashboard_user, employee_id, report_date) DO UPDATE SET
            employee_name_snapshot = excluded.employee_name_snapshot,
            employee_type_snapshot = excluded.employee_type_snapshot,
            goal_description_snapshot = excluded.goal_description_snapshot,
            target_crawl_count = excluded.target_crawl_count,
            target_comment_count = excluded.target_comment_count,
            risk_level = excluded.risk_level,
            reasoning = excluded.reasoning,
            source_snapshot_json = excluded.source_snapshot_json,
            prompt_id = excluded.prompt_id,
            prompt_updated_at = excluded.prompt_updated_at,
            llm_model = excluded.llm_model,
            prompt_tokens = excluded.prompt_tokens,
            completion_tokens = excluded.completion_tokens,
            total_tokens = excluded.total_tokens,
            updated_at = excluded.updated_at
        """,
        (
            str(username),
            str(record.get("employee_id", "")),
            str(record.get("employee_name_snapshot", "")),
            str(record.get("employee_type_snapshot", "")),
            str(record.get("goal_description_snapshot", "")),
            _report_date_to_str(record.get("report_date")),
            _coerce_non_negative_int(record.get("target_crawl_count", 0)),
            _coerce_non_negative_int(record.get("target_comment_count", 0)),
            _normalize_risk_level(record.get("risk_level")),
            str(record.get("reasoning", "")).strip(),
            _json_dumps_compact(record.get("source_snapshot", {})),
            str(record.get("prompt_id", "")),
            str(record.get("prompt_updated_at", "")),
            str(record.get("llm_model", "")),
            _coerce_non_negative_int(record.get("prompt_tokens", 0)),
            _coerce_non_negative_int(record.get("completion_tokens", 0)),
            _coerce_non_negative_int(record.get("total_tokens", 0)),
            str(record.get("created_at") or now),
            now,
        ),
    )
    conn.commit()
    conn.close()

def get_boss_daily_report(username, report_date):
    conn = get_tasks_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM daily_boss_reports
            WHERE dashboard_user = ? AND report_date = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            conn,
            params=(str(username), _report_date_to_str(report_date)),
        )
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return None
    record = df.to_dict("records")[0]
    record["prompt_tokens"] = _coerce_non_negative_int(record.get("prompt_tokens", 0))
    record["completion_tokens"] = _coerce_non_negative_int(record.get("completion_tokens", 0))
    record["total_tokens"] = _coerce_non_negative_int(record.get("total_tokens", 0))
    record["summary"] = _safe_json_loads(record.get("summary_json"), {})
    return record

def upsert_boss_daily_report(username, report):
    now = _now_str()
    record = dict(report or {})
    summary_value = record.get("summary_json", {})
    if isinstance(summary_value, str):
        summary_value = _safe_json_loads(summary_value, {"summary_markdown": str(record.get("summary_markdown", "")).strip()})
    conn = get_tasks_connection()
    conn.execute(
        """
        INSERT INTO daily_boss_reports (
            dashboard_user,
            report_date,
            summary_markdown,
            summary_json,
            employee_count,
            total_target_crawl_count,
            total_target_comment_count,
            total_actual_note_count,
            total_sent_comment_count,
            total_failed_comment_count,
            llm_model,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dashboard_user, report_date) DO UPDATE SET
            summary_markdown = excluded.summary_markdown,
            summary_json = excluded.summary_json,
            employee_count = excluded.employee_count,
            total_target_crawl_count = excluded.total_target_crawl_count,
            total_target_comment_count = excluded.total_target_comment_count,
            total_actual_note_count = excluded.total_actual_note_count,
            total_sent_comment_count = excluded.total_sent_comment_count,
            total_failed_comment_count = excluded.total_failed_comment_count,
            llm_model = excluded.llm_model,
            prompt_tokens = excluded.prompt_tokens,
            completion_tokens = excluded.completion_tokens,
            total_tokens = excluded.total_tokens,
            updated_at = excluded.updated_at
        """,
        (
            str(username),
            _report_date_to_str(record.get("report_date")),
            str(record.get("summary_markdown", "")).strip(),
            _json_dumps_compact(summary_value),
            _coerce_non_negative_int(record.get("employee_count", 0)),
            _coerce_non_negative_int(record.get("total_target_crawl_count", 0)),
            _coerce_non_negative_int(record.get("total_target_comment_count", 0)),
            _coerce_non_negative_int(record.get("total_actual_note_count", 0)),
            _coerce_non_negative_int(record.get("total_sent_comment_count", 0)),
            _coerce_non_negative_int(record.get("total_failed_comment_count", 0)),
            str(record.get("llm_model", "")),
            _coerce_non_negative_int(record.get("prompt_tokens", 0)),
            _coerce_non_negative_int(record.get("completion_tokens", 0)),
            _coerce_non_negative_int(record.get("total_tokens", 0)),
            str(record.get("created_at") or now),
            now,
        ),
    )
    conn.commit()
    conn.close()


def _roadshow_seed_prefix(username, report_date):
    report_date_text = _report_date_to_str(report_date)
    raw = f"{username}|{report_date_text}|roadshow_seed"
    return "rs_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _roadshow_seed_rng(username, report_date):
    report_date_text = _report_date_to_str(report_date)
    raw = f"{username}|{report_date_text}|roadshow_rng"
    seed = int(hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12], 16)
    return random.Random(seed)


def _roadshow_seed_timestamp(base_dt, minute_offset):
    return (base_dt + timedelta(minutes=int(minute_offset))).strftime("%Y-%m-%d %H:%M:%S")


def _ensure_roadshow_accounts(username, seed_prefix, now_text):
    comment_accounts = load_comment_executor_accounts(username)
    existing_map = {
        str(item.get("account_id", "")).strip(): dict(item)
        for item in comment_accounts
        if str(item.get("account_id", "")).strip()
    }
    desired_accounts = [
        {
            "account_id": "xhs_acc_21873",
            "account_name": "内容运营号A",
            "status": "active",
            "notes": "主推账号，稳定执行评论",
            "created_at": now_text,
            "updated_at": now_text,
        },
        {
            "account_id": "xhs_acc_40716",
            "account_name": "内容运营号B",
            "status": "active",
            "notes": "备份账号，高峰时补量",
            "created_at": now_text,
            "updated_at": now_text,
        },
    ]
    for item in desired_accounts:
        account_id = item["account_id"]
        merged = dict(existing_map.get(account_id, {}))
        merged.update(item)
        merged["updated_at"] = now_text
        merged["created_at"] = str(merged.get("created_at") or now_text)
        existing_map[account_id] = merged
    merged_accounts = sorted(
        existing_map.values(),
        key=lambda item: str(item.get("updated_at", "")),
        reverse=True,
    )
    save_comment_executor_accounts_for_user(username, merged_accounts)

    verifier_id = "xhs_verifier_901"
    try:
        upsert_verifier_account(
            username,
            account_id=verifier_id,
            account_name="回溯验号A",
            notes="用于路演场景回溯校验",
            status="active",
            make_default=True,
        )
    except Exception:
        pass

    packages = load_comment_strategy_packages(username)
    package_map = {
        str(item.get("id", "")).strip(): dict(item)
        for item in packages
        if str(item.get("id", "")).strip()
    }
    seed_package_id = f"{seed_prefix}_pkg"
    seed_package = {
        "id": seed_package_id,
        "name": "自然互动稳态包",
        "description": "路演演示：模拟真实运营节奏，控制风控与上评率平衡",
        "daily_limit": 90,
        "hourly_limit": 16,
        "jitter_min_sec": 40,
        "jitter_max_sec": 160,
        "active_start": "09:00",
        "active_end": "22:30",
        "updated_at": now_text,
    }
    existing_package = dict(package_map.get(seed_package_id, {}))
    existing_package.update(seed_package)
    package_map[seed_package_id] = existing_package
    save_comment_strategy_packages_for_user(username, list(package_map.values()))

    templates = load_comment_templates(username)
    template_map = {
        str(item.get("id", "")).strip(): dict(item)
        for item in templates
        if str(item.get("id", "")).strip()
    }
    seed_templates = [
        {
            "id": f"{seed_prefix}_tpl_1",
            "name": "轻互动种草模板",
            "category": "种草型",
            "scene": "笔记互动",
            "content": "这条我也有同感，尤其是你提到的这个细节很实用，我这边实测也确实有效。",
            "emoji": "👏",
            "image_url": "",
            "tags": ["种草", "互动"],
            "status": "active",
            "used_count": 62,
            "created_at": now_text,
            "updated_at": now_text,
            "last_used_at": now_text,
        },
        {
            "id": f"{seed_prefix}_tpl_2",
            "name": "咨询引导模板",
            "category": "引流型",
            "scene": "评论区转私聊",
            "content": "你这个问题问得很准，我整理过一版对比清单，如果你要我可以发你参考。",
            "emoji": "📌",
            "image_url": "",
            "tags": ["引导", "咨询"],
            "status": "active",
            "used_count": 44,
            "created_at": now_text,
            "updated_at": now_text,
            "last_used_at": now_text,
        },
    ]
    for item in seed_templates:
        template_id = item["id"]
        merged = dict(template_map.get(template_id, {}))
        merged.update(item)
        template_map[template_id] = merged
    save_comment_templates_for_user(username, list(template_map.values()))
    return {
        "comment_accounts": merged_accounts,
        "verifier_account_id": verifier_id,
        "strategy_package_id": seed_package_id,
        "strategy_package_name": seed_package["name"],
        "template_ids": [item["id"] for item in seed_templates],
    }


def _ensure_roadshow_employees(username, seed_prefix, base_context, now_text):
    profiles = load_employee_profiles(username)
    profile_map = {str(item.get("id", "")).strip(): dict(item) for item in profiles if str(item.get("id", "")).strip()}
    comment_ids = [
        str(item.get("account_id", "")).strip()
        for item in base_context.get("comment_accounts", [])
        if str(item.get("account_id", "")).strip()
    ]
    package_id = str(base_context.get("strategy_package_id", "")).strip()

    desired = [
        {
            "id": f"{seed_prefix}_emp_seeding",
            "name": "种草增长员-小夏",
            "employee_type": "seeding",
            "status": "active",
            "goal_description": "围绕护肤与通勤穿搭话题，提升高意向用户互动量和收藏率。",
            "work_time_blocks": [
                {"start": "09:00", "end": "12:00"},
                {"start": "14:00", "end": "18:00"},
            ],
            "assigned_crawl_accounts": ["品牌主号A"],
            "assigned_comment_accounts": comment_ids[:1] or comment_ids,
            "assigned_strategy_package_ids": [package_id] if package_id else [],
            "persona_prompt_id": "",
            "planning_prompt_id": "",
            "kpi_prompt_id": "",
            "schedule_prompt_id": "",
            "analysis_scope_mode": "all",
            "analysis_employee_ids": [],
            "analysis_frequency": "daily",
            "notes": "重点观察收藏率和评论正向反馈。",
            "created_at": now_text,
            "updated_at": now_text,
        },
        {
            "id": f"{seed_prefix}_emp_lead",
            "name": "引流转化员-阿泽",
            "employee_type": "lead_generation",
            "status": "active",
            "goal_description": "对高互动笔记做评论承接，提升私信咨询意向并降低无效回复。",
            "work_time_blocks": [
                {"start": "10:00", "end": "12:30"},
                {"start": "15:00", "end": "21:30"},
            ],
            "assigned_crawl_accounts": ["品牌主号B"],
            "assigned_comment_accounts": comment_ids[:2] or comment_ids,
            "assigned_strategy_package_ids": [package_id] if package_id else [],
            "persona_prompt_id": "",
            "planning_prompt_id": "",
            "kpi_prompt_id": "",
            "schedule_prompt_id": "",
            "analysis_scope_mode": "all",
            "analysis_employee_ids": [],
            "analysis_frequency": "daily",
            "notes": "重点关注上评确认率与端到端成功率。",
            "created_at": now_text,
            "updated_at": now_text,
        },
        {
            "id": f"{seed_prefix}_emp_lead_strategy",
            "name": "分析-清衡",
            "employee_type": "strategy_lead",
            "status": "active",
            "goal_description": "汇总各员工表现并输出次日策略建议。",
            "work_time_blocks": [
                {"start": "11:00", "end": "12:00"},
                {"start": "19:00", "end": "21:00"},
            ],
            "assigned_crawl_accounts": [],
            "assigned_comment_accounts": [],
            "assigned_strategy_package_ids": [package_id] if package_id else [],
            "persona_prompt_id": "",
            "planning_prompt_id": "",
            "kpi_prompt_id": "",
            "schedule_prompt_id": "",
            "analysis_scope_mode": "selected",
            "analysis_employee_ids": [f"{seed_prefix}_emp_seeding", f"{seed_prefix}_emp_lead"],
            "analysis_frequency": "daily",
            "notes": "用于路演演示策略复盘。",
            "created_at": now_text,
            "updated_at": now_text,
        },
    ]

    for item in desired:
        employee_id = item["id"]
        merged = dict(profile_map.get(employee_id, {}))
        merged.update(item)
        merged["updated_at"] = now_text
        merged["created_at"] = str(merged.get("created_at") or now_text)
        profile_map[employee_id] = merged

    merged_profiles = sorted(
        profile_map.values(),
        key=lambda item: str(item.get("updated_at", "")),
        reverse=True,
    )
    save_employee_profiles_for_user(username, merged_profiles)
    return [item for item in merged_profiles if str(item.get("id", "")).startswith(seed_prefix)]


def _seed_roadshow_tasks(username, report_date_text, seed_prefix):
    report_day = datetime.strptime(report_date_text, "%Y-%m-%d")
    rows = [
        ("儿童护肤", "general", 80, "Completed", "品牌主号A", "", 1, 55),
        ("通勤穿搭", "popularity_descending", 90, "Completed", "品牌主号B", "", 1, 125),
        ("敏感肌修护", "time_descending", 70, "Running", "品牌主号A", "", 1, 210),
        ("小个子显高", "general", 60, "Completed", "品牌主号B", "", 0, 320),
        ("油皮底妆", "time_descending", 75, "Error", "品牌主号B", "请求频率受限，已自动降速重试", 1, 410),
        ("春季换季护肤", "general", 65, "Completed", "品牌主号A", "", 0, 505),
    ]
    conn = get_tasks_connection()
    try:
        conn.execute(
            "DELETE FROM crawler_tasks WHERE dashboard_user = ? AND filter_condition = ?",
            (str(username), f"{seed_prefix}_roadshow"),
        )
        for keyword, sort_type, max_count, status, account_name, last_error, low_risk_mode, minute_offset in rows:
            created_at = _roadshow_seed_timestamp(report_day.replace(hour=8, minute=30, second=0), minute_offset)
            conn.execute(
                """
                INSERT INTO crawler_tasks (
                    keyword, sort_type, max_count, start_date, end_date, status, account_name,
                    dashboard_user, last_error, filter_condition, low_risk_mode, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    keyword,
                    sort_type,
                    int(max_count),
                    report_date_text,
                    report_date_text,
                    status,
                    account_name,
                    str(username),
                    str(last_error or ""),
                    f"{seed_prefix}_roadshow",
                    int(low_risk_mode),
                    created_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _seed_roadshow_notes(username, report_date_text, seed_prefix):
    rng = _roadshow_seed_rng(username, report_date_text)
    report_day = datetime.strptime(report_date_text, "%Y-%m-%d")
    keywords = ["儿童护肤", "通勤穿搭", "敏感肌修护", "小个子显高", "油皮底妆", "春季换季护肤"]
    cities = ["上海", "杭州", "深圳", "北京", "广州"]
    user_prefixes = ["会买的小宇", "认真做功课", "运营观察局", "日常研究所", "护肤通勤日记"]
    note_records = []
    for idx in range(24):
        raw_key = f"{seed_prefix}|note|{idx}"
        note_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:24]
        keyword = keywords[idx % len(keywords)]
        user_name = user_prefixes[idx % len(user_prefixes)]
        title = f"{keyword}真实反馈第{idx + 1}期：这几个细节决定互动质量"
        desc = (
            f"今天复盘了{keyword}相关内容，发现高互动内容通常都有明确场景和可执行建议。"
            "这条里把踩坑点、替代方案和预算区间都列清楚，评论区咨询明显更集中。"
        )
        note_time = report_day.replace(hour=9, minute=0, second=0) + timedelta(minutes=idx * 21)
        add_ts = int(note_time.timestamp() * 1000)
        modify_ts = int((note_time + timedelta(minutes=8)).timestamp() * 1000)
        liked = 120 + idx * 13 + rng.randint(0, 36)
        collected = 40 + idx * 5 + rng.randint(0, 14)
        comment_count = 18 + idx * 2 + rng.randint(0, 9)
        share_count = 8 + idx + rng.randint(0, 4)
        note_records.append(
            {
                "user_id": f"user_{hashlib.md5(raw_key.encode('utf-8')).hexdigest()[:10]}",
                "nickname": f"{user_name}{idx % 5}",
                "avatar": "https://sns-avatar-qc.xhscdn.com/avatar/default.jpg",
                "ip_location": cities[idx % len(cities)],
                "add_ts": add_ts,
                "last_modify_ts": modify_ts,
                "note_id": note_id,
                "type": "normal",
                "title": title,
                "desc": desc,
                "video_url": "",
                "time": add_ts,
                "last_update_time": modify_ts,
                "liked_count": str(liked),
                "collected_count": str(collected),
                "comment_count": str(comment_count),
                "share_count": str(share_count),
                "image_list": "https://sns-webpic-qc.xhscdn.com/default/demo_note.jpg",
                "tag_list": f"{keyword},运营复盘,评论优化",
                "note_url": f"https://www.xiaohongshu.com/explore/{note_id}",
                "source_keyword": keyword,
                "xsec_token": hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:32],
                "account_name": "品牌主号A" if idx % 2 == 0 else "品牌主号B",
                "comment_status": "Commented" if idx % 3 != 0 else "Uncommented",
                "dashboard_user": str(username),
            }
        )

    conn = get_connection()
    try:
        for row in note_records:
            existing = conn.execute(
                "SELECT id FROM xhs_note WHERE dashboard_user = ? AND note_id = ? LIMIT 1",
                (str(username), str(row["note_id"])),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE xhs_note SET
                        user_id = ?, nickname = ?, avatar = ?, ip_location = ?, add_ts = ?, last_modify_ts = ?,
                        type = ?, title = ?, desc = ?, video_url = ?, time = ?, last_update_time = ?,
                        liked_count = ?, collected_count = ?, comment_count = ?, share_count = ?, image_list = ?,
                        tag_list = ?, note_url = ?, source_keyword = ?, xsec_token = ?, account_name = ?,
                        comment_status = ?
                    WHERE dashboard_user = ? AND note_id = ?
                    """,
                    (
                        row["user_id"], row["nickname"], row["avatar"], row["ip_location"], row["add_ts"],
                        row["last_modify_ts"], row["type"], row["title"], row["desc"], row["video_url"], row["time"],
                        row["last_update_time"], row["liked_count"], row["collected_count"], row["comment_count"],
                        row["share_count"], row["image_list"], row["tag_list"], row["note_url"], row["source_keyword"],
                        row["xsec_token"], row["account_name"], row["comment_status"], str(username), row["note_id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO xhs_note (
                        user_id, nickname, avatar, ip_location, add_ts, last_modify_ts, note_id, type, title,
                        desc, video_url, time, last_update_time, liked_count, collected_count, comment_count,
                        share_count, image_list, tag_list, note_url, source_keyword, xsec_token, account_name,
                        comment_status, dashboard_user
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["user_id"], row["nickname"], row["avatar"], row["ip_location"], row["add_ts"],
                        row["last_modify_ts"], row["note_id"], row["type"], row["title"], row["desc"], row["video_url"],
                        row["time"], row["last_update_time"], row["liked_count"], row["collected_count"],
                        row["comment_count"], row["share_count"], row["image_list"], row["tag_list"], row["note_url"],
                        row["source_keyword"], row["xsec_token"], row["account_name"], row["comment_status"], row["dashboard_user"],
                    ),
                )
        conn.commit()
    finally:
        conn.close()
    return note_records


def _seed_roadshow_events(username, seed_prefix, report_date_text, note_records, strategy_package_name, comment_accounts):
    rng = _roadshow_seed_rng(username, report_date_text)
    base_events = load_comment_strategy_events(username)
    keep_events = [item for item in base_events if str(item.get("_roadshow_seed", "")) != seed_prefix]
    report_day = datetime.strptime(report_date_text, "%Y-%m-%d").replace(hour=9, minute=20, second=0)
    account_names = [
        str(item.get("account_name") or item.get("account_id") or "默认运行时账号")
        for item in comment_accounts
    ] or ["默认运行时账号"]
    template_names = ["轻互动种草模板", "咨询引导模板"]

    seeded = []
    for idx in range(36):
        note = note_records[idx % len(note_records)]
        event_time = report_day + timedelta(minutes=idx * 14 + rng.randint(0, 6))
        status = "sent" if idx % 8 != 0 else "failed"
        account_name = account_names[idx % len(account_names)]
        seeded.append(
            {
                "event_id": f"{seed_prefix}_event_{idx + 1}",
                "dashboard_user": str(username),
                "note_id": note.get("note_id"),
                "target_type": "reply" if idx % 5 == 0 else "note",
                "template_name": template_names[idx % len(template_names)],
                "comment_account_name": account_name,
                "mcp_username": account_name,
                "strategy_package_name": strategy_package_name,
                "jitter_sec": int(rng.randint(35, 170)),
                "status": status,
                "error_message": "发送频率受限，已记录待重试" if status == "failed" else "",
                "result_message": "发送成功，等待回溯确认" if status == "sent" else "",
                "scheduled_for": event_time.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": event_time.strftime("%Y-%m-%d %H:%M:%S"),
                "executed_at": (event_time + timedelta(seconds=rng.randint(10, 80))).strftime("%Y-%m-%d %H:%M:%S"),
                "_roadshow_seed": seed_prefix,
            }
        )
    save_comment_strategy_events_for_user(username, keep_events + seeded)
    return len(seeded)


def _seed_roadshow_comment_jobs(username, seed_prefix, report_date_text, note_records, base_context):
    rng = _roadshow_seed_rng(username, report_date_text)
    report_day = datetime.strptime(report_date_text, "%Y-%m-%d").replace(hour=9, minute=30, second=0)
    account_rows = base_context.get("comment_accounts", [])
    account_pairs = [
        (
            str(item.get("account_id") or MCP_DEFAULT_RUNTIME_ACCOUNT_ID),
            str(item.get("account_name") or item.get("account_id") or "默认运行时账号"),
        )
        for item in account_rows
    ] or [(MCP_DEFAULT_RUNTIME_ACCOUNT_ID, "默认运行时账号")]
    verifier_id = str(base_context.get("verifier_account_id") or "")
    verifier_name = "回溯验号A" if verifier_id else ""
    template_ids = base_context.get("template_ids", [])
    strategy_package_id = str(base_context.get("strategy_package_id") or "")
    strategy_package_name = str(base_context.get("strategy_package_name") or "默认策略包")

    status_cycle = [
        "queued",
        "queued",
        "verify_pending",
        "verified_visible",
        "verified_hidden",
        "send_failed",
        "manual_review",
    ]
    conn = get_tasks_connection()
    created_job_ids = []
    try:
        old_rows = conn.execute(
            "SELECT id FROM comment_jobs WHERE dashboard_user = ? AND request_id LIKE ?",
            (str(username), f"{seed_prefix}_%"),
        ).fetchall()
        old_ids = [int(item[0]) for item in old_rows]
        if old_ids:
            placeholders = ",".join(["?"] * len(old_ids))
            conn.execute(
                f"DELETE FROM comment_verification_runs WHERE comment_job_id IN ({placeholders})",
                old_ids,
            )
            conn.execute(
                f"DELETE FROM comment_jobs WHERE id IN ({placeholders})",
                old_ids,
            )

        for idx in range(28):
            note = note_records[idx % len(note_records)]
            status = status_cycle[idx % len(status_cycle)]
            account_id, account_name = account_pairs[idx % len(account_pairs)]
            plan_dt = report_day + timedelta(minutes=idx * 11 + rng.randint(0, 5))
            execute_dt = plan_dt + timedelta(seconds=rng.randint(20, 140))
            content = (
                "这条内容的结构很清楚，尤其是步骤拆解部分很友好。"
                if idx % 2 == 0
                else "补充一个实测点：同类场景下先做预算分层，互动质量会稳定很多。"
            )
            template_id = template_ids[idx % len(template_ids)] if template_ids else ""
            template_name = "轻互动种草模板" if template_id.endswith("_1") else "咨询引导模板"
            request_id = f"{seed_prefix}_{idx + 1}"
            verification_reason = ""
            last_error = ""
            result_message = "接口发送成功，待回溯"
            posted_comment_id = ""
            verified_comment_id = ""
            verified_comment_content = ""
            verified_comment_like_count = 0
            last_verified_at = ""
            verification_attempt_count = 0
            send_attempt_count = 1
            send_response = {"code": 0, "message": "ok"}
            if status == "send_failed":
                last_error = "接口限流，已自动降速并标记可重试"
                result_message = "发送失败"
                send_response = {"code": 429, "message": "rate_limited"}
            elif status in {"verify_pending", "verified_visible", "verified_hidden", "manual_review"}:
                posted_comment_id = f"pc_{hashlib.md5(request_id.encode('utf-8')).hexdigest()[:10]}"
                last_verified_at = (execute_dt + timedelta(minutes=19)).strftime("%Y-%m-%d %H:%M:%S")
                verification_attempt_count = 1
                if status == "verified_visible":
                    verified_comment_id = posted_comment_id
                    verified_comment_content = content
                    verified_comment_like_count = int(rng.randint(2, 19))
                    verification_reason = "已在目标笔记评论区命中，且内容一致"
                elif status == "verified_hidden":
                    verification_reason = "接口发送成功，但当前未检索到可见评论"
                elif status == "manual_review":
                    verification_reason = "命中内容相似度不足，建议人工复核"

            cursor = conn.execute(
                """
                INSERT INTO comment_jobs (
                    dashboard_user, note_id, note_title, note_url, xsec_token, target_type,
                    target_comment_id, target_user_id, target_nickname, target_comment_preview,
                    template_id, template_name, strategy_package_id, strategy_package_name,
                    comment_account_id, comment_account_name, verifier_account_id, verifier_account_name,
                    comment_content, comment_content_hash, status, scheduled_for, strategy_suggested_for,
                    strategy_delayed, jitter_sec, send_attempt_count, verification_attempt_count,
                    request_id, mcp_username, posted_comment_id, verified_comment_id, verified_comment_content,
                    verified_comment_like_count, verification_reason, last_error, result_message, payload_json,
                    send_response_json, queued_at, started_at, executed_at, last_verified_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(username),
                    str(note.get("note_id")),
                    str(note.get("title")),
                    str(note.get("note_url")),
                    str(note.get("xsec_token")),
                    "reply" if idx % 6 == 0 else "note",
                    "",
                    "",
                    "",
                    "",
                    str(template_id),
                    template_name,
                    strategy_package_id,
                    strategy_package_name,
                    account_id,
                    account_name,
                    verifier_id,
                    verifier_name,
                    content,
                    _hash_comment_content(content),
                    status,
                    plan_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    plan_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    0,
                    int(rng.randint(35, 170)),
                    int(send_attempt_count),
                    int(verification_attempt_count),
                    request_id,
                    account_name,
                    posted_comment_id,
                    verified_comment_id,
                    verified_comment_content,
                    int(verified_comment_like_count),
                    verification_reason,
                    last_error,
                    result_message,
                    _json_dumps_compact({"note_id": note.get("note_id"), "target": "roadshow"}),
                    _json_dumps_compact(send_response),
                    plan_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    plan_dt.strftime("%Y-%m-%d %H:%M:%S") if status != "queued" else "",
                    execute_dt.strftime("%Y-%m-%d %H:%M:%S") if status != "queued" else "",
                    last_verified_at,
                    plan_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    execute_dt.strftime("%Y-%m-%d %H:%M:%S") if status != "queued" else plan_dt.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            job_id = int(cursor.lastrowid)
            created_job_ids.append((job_id, status, content, account_name, verifier_id, verifier_name, execute_dt))
        conn.commit()

        for job_id, status, content, account_name, v_id, v_name, execute_dt in created_job_ids:
            if status not in {"verified_visible", "verified_hidden", "manual_review"}:
                continue
            verdict = "visible" if status == "verified_visible" else "hidden" if status == "verified_hidden" else "manual_review"
            reason = (
                "命中同内容评论，确认已上评"
                if verdict == "visible"
                else "未在可见评论中命中，可能折叠或风控"
                if verdict == "hidden"
                else "已命中疑似评论，需要人工复核上下文"
            )
            verified_at = (execute_dt + timedelta(minutes=22)).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """
                INSERT INTO comment_verification_runs (
                    dashboard_user, comment_job_id, verifier_account_id, verifier_account_name, status, verdict,
                    matched_comment_id, matched_comment_content, matched_comment_like_count, matched_comment_create_time,
                    matched_by, reason, error_message, result_json, created_at, verified_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(username),
                    int(job_id),
                    v_id,
                    v_name,
                    "done",
                    verdict,
                    f"cm_{hashlib.sha1(f'{seed_prefix}_{job_id}'.encode('utf-8')).hexdigest()[:9]}",
                    content if verdict != "hidden" else "",
                    int(rng.randint(1, 16)) if verdict != "hidden" else 0,
                    verified_at,
                    "content_hash" if verdict == "visible" else "fuzzy_match" if verdict == "manual_review" else "not_found",
                    reason,
                    "",
                    _json_dumps_compact({"verdict": verdict, "reason": reason}),
                    verified_at,
                    verified_at,
                    verified_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return len(created_job_ids)


def _seed_roadshow_exceptions(username, seed_prefix, report_date_text):
    report_day = datetime.strptime(report_date_text, "%Y-%m-%d").replace(hour=11, minute=0, second=0)
    rows = load_operation_exceptions(username)
    rows = [item for item in rows if str(item.get("_roadshow_seed", "")) != seed_prefix]
    seeded = [
        {
            "exception_id": f"{seed_prefix}_exc_1",
            "occurred_at": _roadshow_seed_timestamp(report_day, 0),
            "source_module": "comment_send",
            "severity": "warn",
            "status": "resolved",
            "employee_id": f"{seed_prefix}_emp_lead",
            "employee_name": "引流转化员-阿泽",
            "task_id": None,
            "job_id": None,
            "note_id": "",
            "account_id": "xhs_acc_40716",
            "strategy_package_id": f"{seed_prefix}_pkg",
            "error_code": "rate_limit",
            "error_message_raw": "发送频率受限",
            "error_message_normalized": "发送频率受限，已自动降速重试",
            "guidance": "已自动重试成功，持续观察账号频率。",
            "owner": "系统",
            "resolved_at": _roadshow_seed_timestamp(report_day, 18),
            "created_at": _roadshow_seed_timestamp(report_day, 0),
            "updated_at": _roadshow_seed_timestamp(report_day, 18),
            "_roadshow_seed": seed_prefix,
        },
        {
            "exception_id": f"{seed_prefix}_exc_2",
            "occurred_at": _roadshow_seed_timestamp(report_day, 40),
            "source_module": "comment_verify",
            "severity": "critical",
            "status": "open",
            "employee_id": f"{seed_prefix}_emp_lead",
            "employee_name": "引流转化员-阿泽",
            "task_id": None,
            "job_id": None,
            "note_id": "",
            "account_id": "xhs_verifier_901",
            "strategy_package_id": f"{seed_prefix}_pkg",
            "error_code": "verify_timeout",
            "error_message_raw": "回溯接口超时",
            "error_message_normalized": "回溯接口超时，部分任务未完成校验",
            "guidance": "建议在低峰期补跑回溯任务，优先处理高价值评论。",
            "owner": "",
            "resolved_at": "",
            "created_at": _roadshow_seed_timestamp(report_day, 40),
            "updated_at": _roadshow_seed_timestamp(report_day, 40),
            "_roadshow_seed": seed_prefix,
        },
    ]
    rows.extend(seeded)
    save_operation_exceptions_for_user(username, rows)
    return len(seeded)


def _seed_roadshow_snapshots_and_report(username, report_date_text, employees):
    now_text = _now_str()
    seeded_snapshots = 0
    for item in employees:
        employee_id = str(item.get("id", ""))
        employee_type = str(item.get("employee_type", "seeding"))
        if employee_type == "strategy_lead":
            target_crawl = 0
            target_comment = 0
            risk_level = "low"
            reasoning = "分析员工以复盘与调度为主，不承担直接抓取与评论配额。"
            resource_summary = {
                "assigned_crawl_account_count": 0,
                "assigned_comment_account_count": 0,
                "active_comment_account_count": 0,
                "inactive_comment_account_count": 0,
                "strategy_packages": [],
                "comment_daily_capacity_upper_bound": 0,
                "comment_hourly_capacity_upper_bound": 0,
                "crawl_capacity_upper_bound": 0,
            }
            activity_summary = {
                "task_created_count": 2,
                "task_completed_count": 2,
                "task_running_count": 0,
                "task_failed_count": 0,
                "actual_note_count": 0,
                "interaction_total": 0,
                "top_keywords": {},
                "sent_comment_count": 0,
                "failed_comment_count": 0,
                "recent_comment_event_time": now_text,
            }
        else:
            target_crawl = 120 if employee_type == "seeding" else 85
            target_comment = 46 if employee_type == "seeding" else 58
            risk_level = "medium" if employee_type == "lead_generation" else "low"
            reasoning = (
                "评论账号与策略包容量匹配，建议维持中等节奏持续放量，先稳住上评确认率。"
                if employee_type == "lead_generation"
                else "内容供给稳定且互动增长明显，建议继续按当前节奏执行并小步优化模板。"
            )
            resource_summary = {
                "assigned_crawl_account_count": max(1, len(item.get("assigned_crawl_accounts", []))),
                "assigned_comment_account_count": max(1, len(item.get("assigned_comment_accounts", []))),
                "active_comment_account_count": max(1, len(item.get("assigned_comment_accounts", []))),
                "inactive_comment_account_count": 0,
                "strategy_packages": [
                    {"id": f"{_roadshow_seed_prefix(username, report_date_text)}_pkg", "name": "自然互动稳态包", "active_window": "09:00-22:30"}
                ],
                "comment_daily_capacity_upper_bound": 90,
                "comment_hourly_capacity_upper_bound": 16,
                "crawl_capacity_upper_bound": 150,
            }
            activity_summary = {
                "task_created_count": 3,
                "task_completed_count": 2,
                "task_running_count": 1,
                "task_failed_count": 0 if employee_type == "seeding" else 1,
                "actual_note_count": 12,
                "interaction_total": 5620 if employee_type == "seeding" else 4870,
                "top_keywords": {"儿童护肤": 6, "通勤穿搭": 4, "敏感肌修护": 2},
                "sent_comment_count": 22 if employee_type == "seeding" else 26,
                "failed_comment_count": 2 if employee_type == "seeding" else 4,
                "recent_comment_event_time": now_text,
            }
        source_snapshot = {
            "report_date": report_date_text,
            "work_summary": {
                "work_time_blocks": item.get("work_time_blocks", []),
                "work_window_summary": _format_work_time_blocks(item.get("work_time_blocks", [])),
            },
            "resource_summary": resource_summary,
            "activity_summary": activity_summary,
            "risk_signals": {
                "has_failed_tasks_today": bool(activity_summary.get("task_failed_count", 0) > 0),
                "has_failed_comments_today": bool(activity_summary.get("failed_comment_count", 0) > 0),
                "missing_crawl_accounts": False,
                "missing_comment_accounts": False,
                "missing_strategy_packages": False,
            },
            "bound_prompt": {
                "prompt_id": "",
                "prompt_name": "",
                "prompt_updated_at": now_text,
            },
        }
        upsert_employee_kpi_snapshot(
            username,
            {
                "employee_id": employee_id,
                "employee_name_snapshot": str(item.get("name", "")),
                "employee_type_snapshot": employee_type,
                "goal_description_snapshot": str(item.get("goal_description", "")),
                "report_date": report_date_text,
                "target_crawl_count": target_crawl,
                "target_comment_count": target_comment,
                "risk_level": risk_level,
                "reasoning": reasoning,
                "source_snapshot": source_snapshot,
                "prompt_id": "",
                "prompt_updated_at": now_text,
                "llm_model": "roadshow-seed",
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "created_at": now_text,
            },
        )
        seeded_snapshots += 1

    summary = get_boss_dashboard_summary(username, report_date_text)
    headline = "今日运营链路稳定，互动效率持续提升"
    summary_markdown = (
        f"## {headline}\n\n"
        f"- 今日员工在岗：{summary.get('active_employee_count', 0)} / {summary.get('employee_count', 0)}\n"
        f"- 今日新增笔记：{summary.get('note_count', 0)} 条，累计互动量：{summary.get('interaction_total', 0)}\n"
        f"- 评论发送：成功 {summary.get('sent_comment_count', 0)} 条，失败 {summary.get('failed_comment_count', 0)} 条\n"
        f"- 今日异常：{summary.get('exception_total_count', 0)} 条，阻塞未处理：{summary.get('exception_critical_open_count', 0)} 条\n\n"
        "### 明日建议\n"
        "- 优先处理阻塞异常，再放大评论节奏。\n"
        "- 保持高表现关键词内容池更新，防止素材衰减。\n"
        "- 对高表现模板继续小步优化，提升上评确认率。"
    )
    upsert_boss_daily_report(
        username,
        {
            "report_date": report_date_text,
            "summary_markdown": summary_markdown,
            "summary_json": {
                "headline": headline,
                "risks": ["回溯校验偶发超时，需在低峰期补跑。"],
                "next_actions": ["先消化阻塞异常，再提升发送节奏。"],
                "employee_highlights": [
                    {
                        "employee_name": "种草增长员-小夏",
                        "summary": "内容互动表现稳定，收藏率持续提升。",
                        "risk_level": "low",
                    },
                    {
                        "employee_name": "引流转化员-阿泽",
                        "summary": "评论承接效率较高，待回溯队列需继续清理。",
                        "risk_level": "medium",
                    },
                ],
            },
            "employee_count": int(summary.get("employee_count", 0)),
            "total_target_crawl_count": int(summary.get("total_target_crawl_count", 0)),
            "total_target_comment_count": int(summary.get("total_target_comment_count", 0)),
            "total_actual_note_count": int(summary.get("note_count", 0)),
            "total_sent_comment_count": int(summary.get("sent_comment_count", 0)),
            "total_failed_comment_count": int(summary.get("failed_comment_count", 0)),
            "llm_model": "roadshow-seed",
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "created_at": now_text,
        },
    )
    return seeded_snapshots


def inject_roadshow_demo_dataset(username):
    now_dt = datetime.now()
    now_text = _now_str()
    report_date_text = now_dt.strftime("%Y-%m-%d")
    seed_prefix = _roadshow_seed_prefix(username, report_date_text)

    base_context = _ensure_roadshow_accounts(username, seed_prefix, now_text)
    employees = _ensure_roadshow_employees(username, seed_prefix, base_context, now_text)
    task_count = _seed_roadshow_tasks(username, report_date_text, seed_prefix)
    note_records = _seed_roadshow_notes(username, report_date_text, seed_prefix)
    event_count = _seed_roadshow_events(
        username,
        seed_prefix,
        report_date_text,
        note_records,
        strategy_package_name=str(base_context.get("strategy_package_name") or "默认策略包"),
        comment_accounts=base_context.get("comment_accounts") or [],
    )
    job_count = _seed_roadshow_comment_jobs(username, seed_prefix, report_date_text, note_records, base_context)
    exception_count = _seed_roadshow_exceptions(username, seed_prefix, report_date_text)
    snapshot_count = _seed_roadshow_snapshots_and_report(username, report_date_text, employees)
    return {
        "report_date": report_date_text,
        "employee_count": len(employees),
        "task_count": task_count,
        "note_count": len(note_records),
        "event_count": event_count,
        "job_count": job_count,
        "exception_count": exception_count,
        "snapshot_count": snapshot_count,
    }


def _strategy_capacity_from_packages(packages, active_comment_account_count):
    limits_daily = sorted(
        [_coerce_non_negative_int(item.get("daily_limit", 0)) for item in packages],
        reverse=True,
    )
    limits_hourly = sorted(
        [_coerce_non_negative_int(item.get("hourly_limit", 0)) for item in packages],
        reverse=True,
    )
    effective_slots = min(len(limits_daily), max(0, int(active_comment_account_count)))
    if effective_slots <= 0:
        return {
            "comment_daily_capacity_upper_bound": 0,
            "comment_hourly_capacity_upper_bound": 0,
        }
    return {
        "comment_daily_capacity_upper_bound": int(sum(limits_daily[:effective_slots])),
        "comment_hourly_capacity_upper_bound": int(sum(limits_hourly[:effective_slots])),
    }

def _build_employee_event_matches(employee_profile, events):
    comment_account_ids = {
        str(item).strip()
        for item in employee_profile.get("assigned_comment_accounts", [])
        if str(item).strip()
    }
    strategy_package_ids = {
        str(item).strip()
        for item in employee_profile.get("assigned_strategy_package_ids", [])
        if str(item).strip()
    }
    if not comment_account_ids and not strategy_package_ids:
        return []

    matched = []
    for event in events:
        if (
            str(event.get("comment_account_id", "")).strip() in comment_account_ids
            or str(event.get("strategy_package_id", "")).strip() in strategy_package_ids
        ):
            matched.append(event)
    return matched

def _filter_operation_exceptions_by_report_date(exceptions, report_date):
    report_date_text = _report_date_to_str(report_date)
    filtered = []
    for item in (exceptions or []):
        dt_value = _to_dt(item.get("occurred_at") or item.get("created_at"))
        if _datetime_in_report_date(dt_value, report_date_text):
            filtered.append(item)
    return filtered

def get_operation_exception_summary(username, report_date):
    report_date_text = _report_date_to_str(report_date)
    exceptions = _filter_operation_exceptions_by_report_date(
        load_operation_exceptions(username),
        report_date_text,
    )
    open_items = [item for item in exceptions if str(item.get("status")) == "open"]
    critical_open = [
        item for item in open_items
        if str(item.get("severity")) == "critical"
    ]
    employee_counts = {}
    for item in open_items:
        name = str(item.get("employee_name") or "未归属员工")
        employee_counts[name] = employee_counts.get(name, 0) + 1
    most_affected_employee = "-"
    most_affected_count = 0
    if employee_counts:
        most_affected_employee, most_affected_count = sorted(
            employee_counts.items(),
            key=lambda row: row[1],
            reverse=True,
        )[0]
    return {
        "total_count": len(exceptions),
        "open_count": len(open_items),
        "critical_open_count": len(critical_open),
        "resolved_count": sum(1 for item in exceptions if str(item.get("status")) == "resolved"),
        "ignored_count": sum(1 for item in exceptions if str(item.get("status")) == "ignored"),
        "most_affected_employee": most_affected_employee,
        "most_affected_count": most_affected_count,
        "open_items": open_items,
        "all_items": exceptions,
    }

def build_employee_kpi_context(username, employee_profile, report_date):
    report_date_text, _, _ = _report_day_bounds(report_date)
    employee_type = str(employee_profile.get("employee_type", "seeding"))
    prompts = load_employee_prompts(username)
    resolved_prompt_id = _resolve_prompt_id(
        prompts,
        "kpi",
        employee_type,
        employee_profile.get("kpi_prompt_id"),
    )
    prompt = _get_prompt_by_id(prompts, resolved_prompt_id)

    all_crawl_accounts = load_accounts()
    all_comment_accounts = load_comment_executor_accounts(username)
    all_strategy_packages = load_comment_strategy_packages(username)

    assigned_crawl_accounts = [
        str(item).strip()
        for item in employee_profile.get("assigned_crawl_accounts", [])
        if str(item).strip()
    ]
    assigned_comment_accounts = [
        str(item).strip()
        for item in employee_profile.get("assigned_comment_accounts", [])
        if str(item).strip()
    ]
    assigned_strategy_package_ids = [
        str(item).strip()
        for item in employee_profile.get("assigned_strategy_package_ids", [])
        if str(item).strip()
    ]

    bound_comment_accounts = [
        item for item in all_comment_accounts
        if str(item.get("account_id", "")).strip() in set(assigned_comment_accounts)
    ]
    active_comment_accounts = [
        item for item in bound_comment_accounts
        if str(item.get("status", "active")) == "active"
    ]
    inactive_comment_accounts = [
        item for item in bound_comment_accounts
        if str(item.get("status", "active")) != "active"
    ]
    bound_strategy_packages = [
        item for item in all_strategy_packages
        if str(item.get("id", "")).strip() in set(assigned_strategy_package_ids)
    ]
    capacity = _strategy_capacity_from_packages(bound_strategy_packages, len(active_comment_accounts))

    notes_df = load_data_for_username(username)
    notes_today_df = _filter_notes_by_report_date(notes_df, report_date_text)
    employee_notes_df = notes_today_df.iloc[0:0].copy()
    if assigned_crawl_accounts and not notes_today_df.empty and "account_name" in notes_today_df.columns:
        employee_notes_df = notes_today_df[
            notes_today_df["account_name"].fillna("").astype(str).isin(assigned_crawl_accounts)
        ].copy()

    if not employee_notes_df.empty:
        interaction_total = int(
            employee_notes_df["liked_count"].apply(parse_metric_value).sum()
            + employee_notes_df["collected_count"].apply(parse_metric_value).sum()
            + employee_notes_df["comment_count"].apply(parse_metric_value).sum()
        )
        top_keywords_series = employee_notes_df["source_keyword"].fillna("").astype(str)
        top_keywords_series = top_keywords_series[top_keywords_series != ""]
        top_keywords = top_keywords_series.value_counts().head(5).to_dict()
    else:
        interaction_total = 0
        top_keywords = {}

    tasks_df = get_tasks_for_user(username)
    task_records = tasks_df.to_dict("records") if not tasks_df.empty else []
    tasks_today = [
        item for item in task_records
        if _datetime_in_report_date(_to_dt(item.get("created_at")), report_date_text)
    ]
    employee_tasks = [
        item for item in tasks_today
        if str(item.get("account_name", "") or "").strip() in set(assigned_crawl_accounts)
    ] if assigned_crawl_accounts else []
    task_created_count = len(employee_tasks)
    task_completed_count = sum(1 for item in employee_tasks if str(item.get("status")) == "Completed")
    task_failed_count = sum(1 for item in employee_tasks if str(item.get("status")) == "Error")
    task_running_count = sum(1 for item in employee_tasks if str(item.get("status")) == "Running")

    comment_events = _filter_events_by_report_date(load_comment_strategy_events(username), report_date_text)
    employee_comment_events = _build_employee_event_matches(employee_profile, comment_events)
    sent_comment_count = sum(
        1 for item in employee_comment_events
        if str(item.get("status", "")).lower() == "sent"
    )
    failed_comment_count = sum(
        1 for item in employee_comment_events
        if str(item.get("status", "")).lower() in {"failed", "error"}
    )
    latest_event_time = None
    for item in employee_comment_events:
        dt_value = _event_time_for_timeline(item)
        if dt_value and (latest_event_time is None or dt_value > latest_event_time):
            latest_event_time = dt_value

    work_time_blocks = employee_profile.get("work_time_blocks", [])
    work_minutes = 0
    for item in work_time_blocks:
        start_h, start_m = _parse_hhmm(item.get("start", "00:00"), 0, 0)
        end_h, end_m = _parse_hhmm(item.get("end", "00:00"), 0, 0)
        work_minutes += max(0, ((end_h * 60 + end_m) - (start_h * 60 + start_m)))

    source_snapshot = {
        "report_date": report_date_text,
        "employee": {
            "employee_id": str(employee_profile.get("id", "")),
            "employee_name": str(employee_profile.get("name", "")),
            "employee_type": employee_type,
            "employee_status": str(employee_profile.get("status", "active")),
            "goal_description": str(employee_profile.get("goal_description", "")),
            "notes": str(employee_profile.get("notes", "")),
        },
        "work_summary": {
            "work_time_blocks": _normalize_work_time_blocks(work_time_blocks),
            "work_window_summary": _format_work_time_blocks(work_time_blocks),
            "work_hour_count": round(work_minutes / 60, 2),
        },
        "resource_summary": {
            "known_crawl_account_count": len(all_crawl_accounts),
            "assigned_crawl_account_count": len(assigned_crawl_accounts),
            "assigned_crawl_accounts": assigned_crawl_accounts,
            "assigned_comment_account_count": len(bound_comment_accounts),
            "active_comment_account_count": len(active_comment_accounts),
            "inactive_comment_account_count": len(inactive_comment_accounts),
            "comment_accounts": [
                {
                    "account_id": str(item.get("account_id", "")),
                    "account_name": str(item.get("account_name", "")),
                    "status": str(item.get("status", "active")),
                }
                for item in bound_comment_accounts
            ],
            "assigned_strategy_package_count": len(bound_strategy_packages),
            "strategy_packages": [
                {
                    "id": str(item.get("id", "")),
                    "name": str(item.get("name", "")),
                    "daily_limit": _coerce_non_negative_int(item.get("daily_limit", 0)),
                    "hourly_limit": _coerce_non_negative_int(item.get("hourly_limit", 0)),
                    "active_window": f"{item.get('active_start', '-')}-{item.get('active_end', '-')}",
                }
                for item in bound_strategy_packages
            ],
            "comment_daily_capacity_upper_bound": capacity["comment_daily_capacity_upper_bound"],
            "comment_hourly_capacity_upper_bound": capacity["comment_hourly_capacity_upper_bound"],
            "crawl_capacity_upper_bound": int(len(assigned_crawl_accounts) * 150),
        },
        "activity_summary": {
            "task_created_count": task_created_count,
            "task_completed_count": task_completed_count,
            "task_running_count": task_running_count,
            "task_failed_count": task_failed_count,
            "actual_note_count": int(len(employee_notes_df)),
            "interaction_total": interaction_total,
            "top_keywords": top_keywords,
            "sent_comment_count": sent_comment_count,
            "failed_comment_count": failed_comment_count,
            "recent_comment_event_time": latest_event_time.strftime("%Y-%m-%d %H:%M:%S") if latest_event_time else "",
        },
        "risk_signals": {
            "has_failed_tasks_today": bool(task_failed_count > 0),
            "has_failed_comments_today": bool(failed_comment_count > 0),
            "missing_crawl_accounts": not bool(assigned_crawl_accounts),
            "missing_comment_accounts": not bool(active_comment_accounts),
            "missing_strategy_packages": not bool(bound_strategy_packages),
        },
        "bound_prompt": {
            "prompt_id": str(prompt.get("id", "")) if prompt else "",
            "prompt_name": str(prompt.get("name", "")) if prompt else "",
            "prompt_updated_at": str(prompt.get("updated_at", "")) if prompt else "",
        },
    }
    return {
        "employee_profile": employee_profile,
        "prompt": prompt,
        "source_snapshot": source_snapshot,
    }

def get_boss_dashboard_summary(username, report_date):
    report_date_text, _, _ = _report_day_bounds(report_date)
    employees = load_employee_profiles(username)
    notes_df = load_data_for_username(username)
    notes_today_df = _filter_notes_by_report_date(notes_df, report_date_text)
    tasks_df = get_tasks_for_user(username)
    task_records = tasks_df.to_dict("records") if not tasks_df.empty else []
    tasks_today = [
        item for item in task_records
        if _datetime_in_report_date(_to_dt(item.get("created_at")), report_date_text)
    ]
    comment_events_today = _filter_events_by_report_date(load_comment_strategy_events(username), report_date_text)
    snapshots = get_employee_kpi_snapshots(username, report_date_text)
    report_row = get_boss_daily_report(username, report_date_text)
    exception_summary = get_operation_exception_summary(username, report_date_text)

    if not notes_today_df.empty:
        total_interaction = int(
            notes_today_df["liked_count"].apply(parse_metric_value).sum()
            + notes_today_df["collected_count"].apply(parse_metric_value).sum()
            + notes_today_df["comment_count"].apply(parse_metric_value).sum()
        )
        top_keyword_series = notes_today_df["source_keyword"].fillna("").astype(str)
        top_keyword_series = top_keyword_series[top_keyword_series != ""]
        top_keywords = top_keyword_series.value_counts().head(5).to_dict()
    else:
        total_interaction = 0
        top_keywords = {}

    total_sent_comment_count = sum(
        1 for item in comment_events_today
        if str(item.get("status", "")).lower() == "sent"
    )
    total_failed_comment_count = sum(
        1 for item in comment_events_today
        if str(item.get("status", "")).lower() in {"failed", "error"}
    )
    total_target_crawl_count = sum(_coerce_non_negative_int(item.get("target_crawl_count", 0)) for item in snapshots)
    total_target_comment_count = sum(_coerce_non_negative_int(item.get("target_comment_count", 0)) for item in snapshots)
    total_tokens = sum(_coerce_non_negative_int(item.get("total_tokens", 0)) for item in snapshots)
    if report_row:
        total_tokens += _coerce_non_negative_int(report_row.get("total_tokens", 0))

    return {
        "report_date": report_date_text,
        "employee_count": len(employees),
        "active_employee_count": sum(1 for item in employees if str(item.get("status", "active")) == "active"),
        "note_count": int(len(notes_today_df)),
        "interaction_total": total_interaction,
        "task_count": len(tasks_today),
        "task_completed_count": sum(1 for item in tasks_today if str(item.get("status")) == "Completed"),
        "task_failed_count": sum(1 for item in tasks_today if str(item.get("status")) == "Error"),
        "sent_comment_count": total_sent_comment_count,
        "failed_comment_count": total_failed_comment_count,
        "total_target_crawl_count": total_target_crawl_count,
        "total_target_comment_count": total_target_comment_count,
        "total_tokens": total_tokens,
        "top_keywords": top_keywords,
        "kpi_snapshot_count": len(snapshots),
        "boss_report_exists": bool(report_row),
        "boss_report_updated_at": str(report_row.get("updated_at", "")) if report_row else "",
        "exception_total_count": exception_summary["total_count"],
        "exception_open_count": exception_summary["open_count"],
        "exception_critical_open_count": exception_summary["critical_open_count"],
        "exception_most_affected_employee": exception_summary["most_affected_employee"],
        "exception_most_affected_count": exception_summary["most_affected_count"],
    }

def generate_employee_kpi_snapshot(username, employee_profile, report_date, force=False):
    report_date_text = _report_date_to_str(report_date)
    existing = get_employee_kpi_snapshot(username, employee_profile.get("id"), report_date_text)
    if existing and not force:
        return existing

    context_bundle = build_employee_kpi_context(username, employee_profile, report_date_text)
    prompt = context_bundle["prompt"]
    if not prompt:
        raise RuntimeError(f"员工[{employee_profile.get('name', '-')}]未绑定可用的动态 KPI Prompt。")

    system_prompt = (
        str(prompt.get("content", "")).strip()
        + "\n\n你只输出 JSON，不要输出额外文字，也不要使用 Markdown 代码块。"
    )
    user_prompt = (
        "请基于以下员工今日运营上下文，给出动态 KPI 判断。\n"
        "返回 JSON 格式：\n"
        '{"target_crawl_count": 0, "target_comment_count": 0, "risk_level": "low|medium|high", "reasoning": "写给老板看的解释"}\n\n'
        f"员工运营上下文：\n{_json_dumps_compact(context_bundle['source_snapshot'])}"
    )
    llm_result = _call_agent_report_llm(system_prompt, user_prompt, temperature=0)
    parsed = _extract_json_payload(llm_result["content"])

    resource_summary = context_bundle["source_snapshot"].get("resource_summary", {})
    max_crawl_target = _coerce_non_negative_int(resource_summary.get("crawl_capacity_upper_bound", 0))
    max_comment_target = _coerce_non_negative_int(resource_summary.get("comment_daily_capacity_upper_bound", 0))
    target_crawl_count = _coerce_non_negative_int(parsed.get("target_crawl_count", 0))
    target_comment_count = _coerce_non_negative_int(parsed.get("target_comment_count", 0))
    target_crawl_count = min(target_crawl_count, max_crawl_target) if max_crawl_target > 0 else 0
    target_comment_count = min(target_comment_count, max_comment_target) if max_comment_target > 0 else 0

    reasoning = str(parsed.get("reasoning", "")).strip()
    if not reasoning:
        reasoning = "模型未返回有效解释，请检查 Prompt 或接口配置。"
    snapshot = {
        "employee_id": str(employee_profile.get("id", "")),
        "employee_name_snapshot": str(employee_profile.get("name", "")),
        "employee_type_snapshot": str(employee_profile.get("employee_type", "")),
        "goal_description_snapshot": str(employee_profile.get("goal_description", "")),
        "report_date": report_date_text,
        "target_crawl_count": target_crawl_count,
        "target_comment_count": target_comment_count,
        "risk_level": _normalize_risk_level(parsed.get("risk_level")),
        "reasoning": reasoning,
        "source_snapshot": context_bundle["source_snapshot"],
        "prompt_id": str(prompt.get("id", "")),
        "prompt_updated_at": str(prompt.get("updated_at", "")),
        "llm_model": llm_result["model"],
        "prompt_tokens": llm_result["usage"]["prompt_tokens"],
        "completion_tokens": llm_result["usage"]["completion_tokens"],
        "total_tokens": llm_result["usage"]["total_tokens"],
        "created_at": existing.get("created_at") if existing else _now_str(),
    }
    upsert_employee_kpi_snapshot(username, snapshot)
    return get_employee_kpi_snapshot(username, employee_profile.get("id"), report_date_text)

def generate_employee_kpi_snapshots_for_report_date(username, report_date, force=False):
    employees = load_employee_profiles(username)
    generated = []
    errors = []
    for employee in employees:
        try:
            generated.append(generate_employee_kpi_snapshot(username, employee, report_date, force=force))
        except Exception as exc:
            errors.append(f"{employee.get('name', '-')}: {exc}")
    return {
        "snapshots": generated,
        "errors": errors,
    }

def build_boss_report_context(username, report_date, snapshots=None):
    report_date_text = _report_date_to_str(report_date)
    snapshots = snapshots if snapshots is not None else get_employee_kpi_snapshots(username, report_date_text)
    summary = get_boss_dashboard_summary(username, report_date_text)
    employees = load_employee_profiles(username)
    notes_df = load_data_for_username(username)
    notes_today_df = _filter_notes_by_report_date(notes_df, report_date_text)
    comment_events_today = _filter_events_by_report_date(load_comment_strategy_events(username), report_date_text)
    tasks_df = get_tasks_for_user(username)
    task_records = tasks_df.to_dict("records") if not tasks_df.empty else []
    tasks_today = [
        item for item in task_records
        if _datetime_in_report_date(_to_dt(item.get("created_at")), report_date_text)
    ]
    exception_summary = get_operation_exception_summary(username, report_date_text)
    strategy_lead_reports = build_strategy_lead_reports(username, report_date_text)

    if not notes_today_df.empty:
        notes_with_interaction = notes_today_df.copy()
        notes_with_interaction["interaction_total"] = (
            notes_with_interaction["liked_count"].apply(parse_metric_value)
            + notes_with_interaction["collected_count"].apply(parse_metric_value)
            + notes_with_interaction["comment_count"].apply(parse_metric_value)
        )
        top_notes_df = notes_with_interaction.sort_values("interaction_total", ascending=False).head(5)
        top_notes = [
            {
                "title": str(item.get("title", "")),
                "source_keyword": str(item.get("source_keyword", "")),
                "interaction_total": int(item.get("interaction_total", 0)),
            }
            for item in top_notes_df.to_dict("records")
        ]
    else:
        top_notes = []

    snapshot_map = {
        str(item.get("employee_id", "")): item
        for item in snapshots
    }
    employee_summaries = []
    for employee in employees:
        snapshot = snapshot_map.get(str(employee.get("id", "")))
        employee_summaries.append(
            {
                "employee_id": str(employee.get("id", "")),
                "employee_name": str(employee.get("name", "")),
                "employee_type": str(employee.get("employee_type", "")),
                "employee_status": str(employee.get("status", "")),
                "goal_description": str(employee.get("goal_description", "")),
                "work_window_summary": _format_work_time_blocks(employee.get("work_time_blocks", [])),
                "target_crawl_count": _coerce_non_negative_int((snapshot or {}).get("target_crawl_count", 0)),
                "target_comment_count": _coerce_non_negative_int((snapshot or {}).get("target_comment_count", 0)),
                "risk_level": _normalize_risk_level((snapshot or {}).get("risk_level")),
                "reasoning": str((snapshot or {}).get("reasoning", "")),
            }
        )

    return {
        "report_date": report_date_text,
        "summary": summary,
        "employee_summaries": employee_summaries,
        "top_keywords": summary.get("top_keywords", {}),
        "top_notes": top_notes,
        "task_summary": {
            "task_count": len(tasks_today),
            "completed_count": sum(1 for item in tasks_today if str(item.get("status")) == "Completed"),
            "failed_count": sum(1 for item in tasks_today if str(item.get("status")) == "Error"),
        },
        "comment_summary": {
            "sent_count": sum(1 for item in comment_events_today if str(item.get("status", "")).lower() == "sent"),
            "failed_count": sum(1 for item in comment_events_today if str(item.get("status", "")).lower() in {"failed", "error"}),
        },
        "exception_summary": {
            "total_count": exception_summary["total_count"],
            "open_count": exception_summary["open_count"],
            "critical_open_count": exception_summary["critical_open_count"],
            "resolved_count": exception_summary["resolved_count"],
            "most_affected_employee": exception_summary["most_affected_employee"],
            "most_affected_count": exception_summary["most_affected_count"],
            "open_top_items": [
                {
                    "occurred_at": str(item.get("occurred_at") or ""),
                    "source_module": str(item.get("source_module") or ""),
                    "severity": str(item.get("severity") or ""),
                    "employee_name": str(item.get("employee_name") or ""),
                    "account_id": str(item.get("account_id") or ""),
                    "error_message": str(item.get("error_message_normalized") or item.get("error_message_raw") or ""),
                    "guidance": str(item.get("guidance") or ""),
                }
                for item in exception_summary["open_items"][:10]
            ],
        },
        "strategy_lead_reports": [
            {
                "employee_id": str(item.get("employee_id", "")),
                "employee_name": str(item.get("employee_name", "")),
                "analysis_scope_mode": str(item.get("analysis_scope_mode", "")),
                "analysis_frequency": str(item.get("analysis_frequency", "")),
                "recommendation": item.get("recommendation", {}),
            }
            for item in strategy_lead_reports
        ],
    }

def _resolve_strategy_lead_targets(strategy_lead_profile, all_profiles):
    if str(strategy_lead_profile.get("employee_type", "")) != "strategy_lead":
        return []
    worker_profiles = [
        item for item in all_profiles
        if str(item.get("employee_type", "")) in {"seeding", "lead_generation"}
    ]
    scope_mode = str(strategy_lead_profile.get("analysis_scope_mode", "all"))
    if scope_mode == "selected":
        selected_ids = {
            str(item).strip()
            for item in strategy_lead_profile.get("analysis_employee_ids", [])
            if str(item).strip()
        }
        selected_workers = [
            item for item in worker_profiles
            if str(item.get("id", "")) in selected_ids
        ]
        if selected_workers:
            return selected_workers
    return worker_profiles

def _build_strategy_lead_context(username, report_date, strategy_lead_profile, all_profiles=None):
    report_date_text = _report_date_to_str(report_date)
    all_profiles = all_profiles if all_profiles is not None else load_employee_profiles(username)
    targets = _resolve_strategy_lead_targets(strategy_lead_profile, all_profiles)
    target_employee_ids = {str(item.get("id", "")) for item in targets if str(item.get("id", "")).strip()}
    target_crawl_accounts = {
        str(acc).strip()
        for item in targets
        for acc in item.get("assigned_crawl_accounts", [])
        if str(acc).strip()
    }
    target_comment_accounts = {
        str(acc).strip()
        for item in targets
        for acc in item.get("assigned_comment_accounts", [])
        if str(acc).strip()
    }
    target_strategy_packages = {
        str(pkg).strip()
        for item in targets
        for pkg in item.get("assigned_strategy_package_ids", [])
        if str(pkg).strip()
    }

    notes_df = load_data_for_username(username)
    notes_today_df = _filter_notes_by_report_date(notes_df, report_date_text)
    if target_crawl_accounts and not notes_today_df.empty and "account_name" in notes_today_df.columns:
        notes_today_df = notes_today_df[
            notes_today_df["account_name"].fillna("").astype(str).isin(target_crawl_accounts)
        ].copy()

    post_type_rows = []
    keyword_rows = []
    post_like_total = 0
    post_interaction_total = 0
    if not notes_today_df.empty:
        notes_today_df = notes_today_df.copy()
        notes_today_df["liked_count_int"] = notes_today_df["liked_count"].apply(parse_metric_value)
        notes_today_df["interaction_total"] = (
            notes_today_df["liked_count"].apply(parse_metric_value)
            + notes_today_df["collected_count"].apply(parse_metric_value)
            + notes_today_df["comment_count"].apply(parse_metric_value)
        )
        post_like_total = int(notes_today_df["liked_count_int"].sum())
        post_interaction_total = int(notes_today_df["interaction_total"].sum())
        type_column = "type" if "type" in notes_today_df.columns else None
        if type_column:
            grouped_type = notes_today_df.groupby(type_column, dropna=False)
            for type_name, group in grouped_type:
                post_type_rows.append(
                    {
                        "post_type": str(type_name or "unknown"),
                        "note_count": int(len(group)),
                        "post_like_total": int(group["liked_count_int"].sum()),
                        "interaction_total": int(group["interaction_total"].sum()),
                    }
                )
        keyword_column = "source_keyword" if "source_keyword" in notes_today_df.columns else None
        if keyword_column:
            grouped_keyword = notes_today_df.groupby(keyword_column, dropna=False)
            for keyword, group in grouped_keyword:
                keyword_text = str(keyword or "").strip() or "未标注关键词"
                keyword_rows.append(
                    {
                        "keyword": keyword_text,
                        "note_count": int(len(group)),
                        "post_like_total": int(group["liked_count_int"].sum()),
                        "interaction_total": int(group["interaction_total"].sum()),
                    }
                )
            keyword_rows = sorted(keyword_rows, key=lambda item: item.get("interaction_total", 0), reverse=True)[:12]

    jobs_df = load_comment_jobs_df(username, limit=5000)
    comment_jobs = jobs_df.to_dict("records") if not jobs_df.empty else []
    scoped_jobs = []
    for job in comment_jobs:
        if not _datetime_in_report_date(_to_dt(job.get("executed_at") or job.get("created_at")), report_date_text):
            continue
        account_id = str(job.get("comment_account_id") or "").strip()
        package_id = str(job.get("strategy_package_id") or "").strip()
        if target_comment_accounts or target_strategy_packages:
            if account_id not in target_comment_accounts and package_id not in target_strategy_packages:
                continue
        scoped_jobs.append(job)

    send_success_count = sum(1 for item in scoped_jobs if str(item.get("status", "")) in COMMENT_JOB_SUCCESS_STATUSES)
    send_failed_count = sum(1 for item in scoped_jobs if str(item.get("status", "")) == "send_failed")
    verified_visible_count = sum(1 for item in scoped_jobs if str(item.get("status", "")) == "verified_visible")
    verified_hidden_count = sum(1 for item in scoped_jobs if str(item.get("status", "")) == "verified_hidden")
    manual_review_count = sum(1 for item in scoped_jobs if str(item.get("status", "")) == "manual_review")
    verified_comment_like_total = sum(
        _coerce_non_negative_int(item.get("verified_comment_like_count", 0))
        for item in scoped_jobs
    )

    template_stats = {}
    hour_stats = {}
    for item in scoped_jobs:
        template_name = str(item.get("template_name") or "未命名模板")
        row = template_stats.setdefault(
            template_name,
            {
                "template_name": template_name,
                "task_count": 0,
                "send_success_count": 0,
                "send_failed_count": 0,
                "verified_visible_count": 0,
                "verified_comment_like_total": 0,
            },
        )
        row["task_count"] += 1
        status = str(item.get("status") or "")
        if status in COMMENT_JOB_SUCCESS_STATUSES:
            row["send_success_count"] += 1
        if status == "send_failed":
            row["send_failed_count"] += 1
        if status == "verified_visible":
            row["verified_visible_count"] += 1
        row["verified_comment_like_total"] += _coerce_non_negative_int(item.get("verified_comment_like_count", 0))

        dt_value = _to_dt(item.get("executed_at") or item.get("created_at"))
        if dt_value:
            hour_key = dt_value.strftime("%H:00")
            hrow = hour_stats.setdefault(hour_key, {"hour": hour_key, "task_count": 0, "verified_visible_count": 0})
            hrow["task_count"] += 1
            if status == "verified_visible":
                hrow["verified_visible_count"] += 1

    template_rows = list(template_stats.values())
    for item in template_rows:
        sent_base = item["send_success_count"] + item["send_failed_count"]
        item["send_success_rate"] = round(item["send_success_count"] / sent_base, 4) if sent_base > 0 else 0.0
        item["visible_rate"] = round(item["verified_visible_count"] / item["send_success_count"], 4) if item["send_success_count"] > 0 else 0.0
        item["avg_verified_comment_like"] = round(item["verified_comment_like_total"] / item["verified_visible_count"], 2) if item["verified_visible_count"] > 0 else 0.0
    template_rows = sorted(template_rows, key=lambda item: (item.get("visible_rate", 0), item.get("avg_verified_comment_like", 0)), reverse=True)
    hour_rows = sorted(list(hour_stats.values()), key=lambda item: item.get("verified_visible_count", 0), reverse=True)
    post_type_rows = sorted(post_type_rows, key=lambda item: item.get("interaction_total", 0), reverse=True)

    exception_rows = _filter_operation_exceptions_by_report_date(load_operation_exceptions(username), report_date_text)
    if target_employee_ids:
        exception_rows = [
            item for item in exception_rows
            if str(item.get("employee_id") or "").strip() in target_employee_ids
        ]

    return {
        "report_date": report_date_text,
        "leader": {
            "employee_id": str(strategy_lead_profile.get("id", "")),
            "employee_name": str(strategy_lead_profile.get("name", "")),
            "analysis_scope_mode": str(strategy_lead_profile.get("analysis_scope_mode", "all")),
            "analysis_frequency": str(strategy_lead_profile.get("analysis_frequency", "daily")),
        },
        "target_employee_count": len(targets),
        "target_employee_names": [str(item.get("name", "")) for item in targets],
        "post_summary": {
            "note_count": int(len(notes_today_df)),
            "post_like_total": post_like_total,
            "interaction_total": post_interaction_total,
            "post_type_rows": post_type_rows[:10],
            "keyword_rows": keyword_rows[:10],
        },
        "comment_summary": {
            "task_count": len(scoped_jobs),
            "send_success_count": send_success_count,
            "send_failed_count": send_failed_count,
            "verified_visible_count": verified_visible_count,
            "verified_hidden_count": verified_hidden_count,
            "manual_review_count": manual_review_count,
            "verified_comment_like_total": verified_comment_like_total,
            "template_rows": template_rows[:12],
            "hour_rows": hour_rows[:12],
        },
        "exception_summary": {
            "total_count": len(exception_rows),
            "open_count": sum(1 for item in exception_rows if str(item.get("status") or "") == "open"),
            "critical_open_count": sum(
                1 for item in exception_rows
                if str(item.get("status") or "") == "open" and str(item.get("severity") or "") == "critical"
            ),
        },
    }

def _build_strategy_lead_recommendation(context):
    post_summary = context.get("post_summary", {})
    comment_summary = context.get("comment_summary", {})
    exception_summary = context.get("exception_summary", {})
    template_rows = comment_summary.get("template_rows", [])
    hour_rows = comment_summary.get("hour_rows", [])
    post_type_rows = post_summary.get("post_type_rows", [])
    keyword_rows = post_summary.get("keyword_rows", [])

    best_tactics = []
    underperforming_tactics = []
    template_suggestions = []
    time_window_suggestions = []
    target_post_type_suggestions = []
    risk_controls = []

    if template_rows:
        best = template_rows[0]
        best_tactics.append(
            f"优先使用「{best.get('template_name', '-')}」：上评率 {best.get('visible_rate', 0):.0%}，评论点赞均值 {best.get('avg_verified_comment_like', 0)}。"
        )
        weak_templates = sorted(template_rows, key=lambda item: (item.get("send_success_rate", 0), item.get("visible_rate", 0)))
        weak = weak_templates[0]
        underperforming_tactics.append(
            f"谨慎使用「{weak.get('template_name', '-')}」：接口成功率 {weak.get('send_success_rate', 0):.0%}，上评率 {weak.get('visible_rate', 0):.0%}。"
        )
        template_suggestions.append(
            f"新增 1 条与「{best.get('template_name', '-')}」结构相似的话术，保留共情开场+结论引导。"
        )
        if weak.get("task_count", 0) >= 3:
            template_suggestions.append(f"下调或停用「{weak.get('template_name', '-')}」，优先替换为高上评模板。")
    else:
        risk_controls.append("暂无有效模板表现数据，先完成发送与回溯闭环后再优化模板。")

    if hour_rows:
        top_hour = hour_rows[0]
        time_window_suggestions.append(
            f"优先在 {top_hour.get('hour', '-') } 时段执行评论，该时段已确认上评数最高。"
        )
    else:
        time_window_suggestions.append("暂无稳定时段结论，建议先按工作窗口均匀分布采样。")

    if post_type_rows:
        top_type = post_type_rows[0]
        target_post_type_suggestions.append(
            f"优先跟进「{top_type.get('post_type', '-')}」帖子，当前互动量最高。"
        )
    if keyword_rows:
        top_keyword = keyword_rows[0]
        target_post_type_suggestions.append(
            f"优先扩展关键词「{top_keyword.get('keyword', '-')}」相关帖子，当前关键词互动表现最好。"
        )

    send_failed_count = _coerce_non_negative_int(comment_summary.get("send_failed_count", 0))
    if send_failed_count > 0:
        risk_controls.append(f"今日评论发送失败 {send_failed_count} 条，建议先消化失败队列再放量。")
    critical_open = _coerce_non_negative_int(exception_summary.get("critical_open_count", 0))
    if critical_open > 0:
        risk_controls.append(f"存在 {critical_open} 条阻塞异常未处理，建议优先处理阻塞项。")
    if not risk_controls:
        risk_controls.append("当前无明显阻塞异常，建议小步迭代模板并持续观察上评率。")

    summary_markdown = (
        f"### 分析建议\n"
        f"- 覆盖员工数：{context.get('target_employee_count', 0)}\n"
        f"- 帖子点赞总量：{post_summary.get('post_like_total', 0)}\n"
        f"- 评论点赞总量：{comment_summary.get('verified_comment_like_total', 0)}\n"
    )
    return {
        "summary_markdown": summary_markdown,
        "best_tactics": best_tactics,
        "underperforming_tactics": underperforming_tactics,
        "template_suggestions": template_suggestions,
        "time_window_suggestions": time_window_suggestions,
        "target_post_type_suggestions": target_post_type_suggestions,
        "risk_controls": risk_controls,
    }

def build_strategy_lead_reports(username, report_date):
    report_date_text = _report_date_to_str(report_date)
    all_profiles = load_employee_profiles(username)
    strategy_leads = [
        item for item in all_profiles
        if str(item.get("employee_type", "")) == "strategy_lead"
    ]
    reports = []
    for profile in strategy_leads:
        context = _build_strategy_lead_context(
            username,
            report_date_text,
            profile,
            all_profiles=all_profiles,
        )
        recommendation = _build_strategy_lead_recommendation(context)
        reports.append(
            {
                "employee_id": str(profile.get("id", "")),
                "employee_name": str(profile.get("name", "")),
                "analysis_scope_mode": str(profile.get("analysis_scope_mode", "all")),
                "analysis_frequency": str(profile.get("analysis_frequency", "daily")),
                "context": context,
                "recommendation": recommendation,
            }
        )
    return reports

def generate_boss_daily_report(username, report_date, force=False, snapshots=None):
    report_date_text = _report_date_to_str(report_date)
    existing = get_boss_daily_report(username, report_date_text)
    if existing and not force:
        return existing

    snapshots = snapshots if snapshots is not None else get_employee_kpi_snapshots(username, report_date_text)
    if not snapshots:
        raise RuntimeError("请先生成当日员工 KPI 判断，再生成老板日报。")

    context = build_boss_report_context(username, report_date_text, snapshots=snapshots)
    system_prompt = textwrap.dedent(
        """
        你是一名老板专属运营汇报助手。
        你的任务是根据员工 KPI 快照和当天运营数据，生成一份简洁、可信、可行动的老板日报。
        只输出 JSON，不要输出额外文字。
        JSON 格式：
        {
          "summary_markdown": "给老板看的 Markdown 日报正文",
          "headline": "一句话标题",
          "risks": ["风险1", "风险2"],
          "next_actions": ["动作1", "动作2"],
          "employee_highlights": [
            {"employee_name": "员工A", "summary": "今天做了什么", "risk_level": "low|medium|high"}
          ]
        }
        """
    ).strip()
    user_prompt = (
        "请基于以下上下文生成老板日报，语言简洁但要明确说明 KPI 是如何受资源、策略包、任务失败和工作时段影响的。\n\n"
        f"日报上下文：\n{_json_dumps_compact(context)}"
    )
    llm_result = _call_agent_report_llm(system_prompt, user_prompt, temperature=0.1)
    parsed = _extract_json_payload(llm_result["content"])
    summary_markdown = str(parsed.get("summary_markdown", "")).strip()
    if not summary_markdown:
        headline = str(parsed.get("headline", "今日日报")).strip() or "今日日报"
        risks = parsed.get("risks") or []
        next_actions = parsed.get("next_actions") or []
        summary_markdown = f"## {headline}\n"
        if risks:
            summary_markdown += "\n**主要风险**\n" + "\n".join(f"- {item}" for item in risks if str(item).strip())
        if next_actions:
            summary_markdown += "\n\n**明日建议**\n" + "\n".join(f"- {item}" for item in next_actions if str(item).strip())

    summary = get_boss_dashboard_summary(username, report_date_text)
    report_record = {
        "report_date": report_date_text,
        "summary_markdown": summary_markdown,
        "summary_json": parsed,
        "employee_count": summary["employee_count"],
        "total_target_crawl_count": summary["total_target_crawl_count"],
        "total_target_comment_count": summary["total_target_comment_count"],
        "total_actual_note_count": summary["note_count"],
        "total_sent_comment_count": summary["sent_comment_count"],
        "total_failed_comment_count": summary["failed_comment_count"],
        "llm_model": llm_result["model"],
        "prompt_tokens": llm_result["usage"]["prompt_tokens"],
        "completion_tokens": llm_result["usage"]["completion_tokens"],
        "total_tokens": llm_result["usage"]["total_tokens"],
        "created_at": existing.get("created_at") if existing else _now_str(),
    }
    upsert_boss_daily_report(username, report_record)
    return get_boss_daily_report(username, report_date_text)

def build_boss_timeline(username, report_date, snapshots=None, boss_report=None):
    report_date_text = _report_date_to_str(report_date)
    snapshots = snapshots if snapshots is not None else get_employee_kpi_snapshots(username, report_date_text)
    boss_report = boss_report if boss_report is not None else get_boss_daily_report(username, report_date_text)

    timeline = []
    tasks_df = get_tasks_for_user(username)
    task_records = tasks_df.to_dict("records") if not tasks_df.empty else []
    for task in task_records:
        task_dt = _to_dt(task.get("created_at"))
        if not _datetime_in_report_date(task_dt, report_date_text):
            continue
        timeline.append(
            {
                "时间": _format_dt_display(task_dt),
                "分类": "系统执行事件",
                "标题": f"创建爬取任务：{task.get('keyword', '-')}",
                "详情": f"状态：{task.get('status', '-')}｜账号：{task.get('account_name', '-')}",
                "_sort_dt": task_dt,
            }
        )

    for event in _filter_events_by_report_date(load_comment_strategy_events(username), report_date_text):
        event_dt = _event_time_for_timeline(event)
        timeline.append(
            {
                "时间": _format_dt_display(event_dt),
                "分类": "系统执行事件",
                "标题": f"评论事件：{format_comment_event_status(event.get('status', '-'))}",
                "详情": f"{format_comment_event_target(event)}｜模板：{event.get('template_name', '-')}",
                "_sort_dt": event_dt or datetime.now(),
            }
        )

    for item in _filter_operation_exceptions_by_report_date(load_operation_exceptions(username), report_date_text):
        item_dt = _to_dt(item.get("occurred_at") or item.get("created_at"))
        timeline.append(
            {
                "时间": _format_dt_display(item_dt),
                "分类": "异常事件",
                "标题": (
                    f"{_operation_exception_source_label(item.get('source_module'))}"
                    f"｜{_operation_exception_severity_label(item.get('severity'))}"
                ),
                "详情": (
                    f"{item.get('employee_name') or '未归属员工'}｜"
                    f"{item.get('error_message_normalized') or item.get('error_message_raw') or '-'}"
                ),
                "_sort_dt": item_dt or datetime.now(),
            }
        )

    for snapshot in snapshots:
        created_dt = _to_dt(snapshot.get("updated_at") or snapshot.get("created_at"))
        timeline.append(
            {
                "时间": _format_dt_display(created_dt),
                "分类": "员工判断事件",
                "标题": f"KPI 判断：{snapshot.get('employee_name_snapshot', '-')}",
                "详情": (
                    f"抓取目标 {snapshot.get('target_crawl_count', 0)}｜"
                    f"评论目标 {snapshot.get('target_comment_count', 0)}｜"
                    f"{_risk_level_label(snapshot.get('risk_level'))}"
                ),
                "_sort_dt": created_dt or datetime.now(),
            }
        )

    if boss_report:
        report_dt = _to_dt(boss_report.get("updated_at") or boss_report.get("created_at"))
        timeline.append(
            {
                "时间": _format_dt_display(report_dt),
                "分类": "员工判断事件",
                "标题": "老板日报生成",
                "详情": f"模型：{boss_report.get('llm_model', '-')}｜Token：{_format_token_usage(boss_report.get('total_tokens', 0))}",
                "_sort_dt": report_dt or datetime.now(),
            }
        )

    timeline = sorted(timeline, key=lambda item: item.get("_sort_dt") or datetime.min, reverse=True)
    for item in timeline:
        item.pop("_sort_dt", None)
    return timeline

def render_comment_node(note, comment, children_map, depth=0, node_path="0"):
    indent = "　" * depth
    nickname = comment.get("nickname") or "匿名用户"
    content = comment.get("content") or ""
    create_time = format_timestamp(comment.get("create_time"))
    like_count = int(parse_metric_value(comment.get("like_count")))
    sub_count = int(parse_metric_value(comment.get("sub_comment_count")))
    can_reply = bool(
        normalize_comment_id(comment.get("comment_id"))
        or normalize_comment_id(comment.get("user_id"))
    )

    header_col, action_col = st.columns([9, 2])
    with header_col:
        st.markdown(f"{indent}**{nickname}** · {create_time} · 👍 {like_count}")
    with action_col:
        if st.button(
            "回复",
            key=f"reply_comment_{note.get('note_id')}_{node_path}",
            use_container_width=True,
            disabled=not can_reply,
        ):
            open_reply_comment_dialog(note.get("note_id"), comment, node_path)
            st.rerun()

    st.markdown(f"{indent}{content}")

    if sub_count > 0:
        st.caption(f"{indent}子评论数：{sub_count}")

    current_id = normalize_comment_id(comment.get("comment_id"))
    children = children_map.get(current_id, [])
    for child_idx, child in enumerate(children, start=1):
        render_comment_node(note, child, children_map, depth=depth + 1, node_path=f"{node_path}_{child_idx}")

def render_comment_details(note):
    note_id = note.get("note_id")
    comments_df = get_comments(note_id, limit=50)
    action_cols = st.columns([1.4, 3.6])
    if action_cols[0].button("评论整帖", key=f"detail_comment_note_{note_id}", type="primary", use_container_width=True):
        open_note_comment_dialog(note_id)
        st.rerun()
    if comments_df.empty:
        st.caption("暂无评论数据，仍可直接评论整篇笔记。")
        return

    comments = comments_df.to_dict("records")
    comment_id_set = {
        normalize_comment_id(c.get("comment_id"))
        for c in comments
        if normalize_comment_id(c.get("comment_id"))
    }

    children_map = {}
    roots = []
    for c in comments:
        current_id = normalize_comment_id(c.get("comment_id"))
        parent_id = normalize_comment_id(c.get("parent_comment_id"))
        if parent_id and parent_id in comment_id_set and parent_id != current_id:
            children_map.setdefault(parent_id, []).append(c)
        else:
            roots.append(c)

    st.caption("最多展示 50 条，支持直接评论整帖，或对任意一条评论发起回复。")

    for idx, root in enumerate(roots, start=1):
        root_nickname = root.get("nickname") or "匿名用户"
        root_time = format_timestamp(root.get("create_time"))
        root_likes = int(parse_metric_value(root.get("like_count")))
        root_preview = (root.get("content") or "").replace("\n", " ")[:28]
        title = f"#{idx} {root_nickname} · {root_time} · 👍 {root_likes} · {root_preview}"
        with st.expander(title, expanded=False):
            render_comment_node(note, root, children_map, depth=0, node_path=str(idx))

@st.dialog("笔记详情")
def show_note_detail_dialog(note):
    left_col, right_col = st.columns([1.7, 1.3], gap="large")

    with left_col:
        st.markdown("### 笔记内容")
        st.markdown(f"**标题：** {note.get('title') or '-'}")
        if note.get("note_url"):
            st.markdown(f"**链接：** [查看原文]({note['note_url']})")
        st.markdown(f"**博主：** {note.get('nickname') or '-'}")
        st.markdown(f"**发布时间：** {format_timestamp(note.get('time'))}")
        st.markdown(
            f"**互动数据：** ❤️ {int(parse_metric_value(note.get('liked_count'))):,}  "
            f"⭐ {int(parse_metric_value(note.get('collected_count'))):,}  "
            f"💬 {int(parse_metric_value(note.get('comment_count'))):,}"
        )

        if note.get("desc"):
            st.markdown("**正文摘要：**")
            st.write(note.get("desc"))

        image_urls = parse_image_list(note.get("image_list"))
        st.markdown("**截图内容：**")
        if image_urls:
            st.image(image_urls, use_container_width=True)
        else:
            st.caption("暂无可展示截图（该笔记未保存图片或图片链接不可用）。")

    with right_col:
        st.markdown("### 评论明细")
        render_comment_details(note)

@st.dialog("选择评论话术模板")
def show_comment_template_dialog(note):
    note_id = str(note.get("note_id") or "")
    xsec_token = _get_note_xsec_token(note)
    st.markdown(f"**目标笔记：** {note.get('title') or '-'}")

    if not note_id:
        st.error("目标笔记 ID 缺失，无法发起评论。")
        if st.button("关闭", key="close_comment_dialog_missing_note_id"):
            clear_comment_dialog_state()
            st.rerun()
        return

    templates = load_comment_templates(st.session_state.username)
    strategy_packages = load_comment_strategy_packages(st.session_state.username)
    comment_accounts = load_comment_executor_accounts(st.session_state.username)
    verifier_accounts = load_verifier_accounts(st.session_state.username, active_only=True)
    using_runtime_comment_account = False
    if not comment_accounts:
        comment_accounts = [{
            "account_id": "mcp_runtime",
            "account_name": "MCP 当前登录账号",
        }]
        using_runtime_comment_account = True

    if not templates:
        st.info("暂无可用话术模板，请先到“话术管理”页创建模板。")
        if st.button("关闭", key=f"close_comment_dialog_empty_{note_id}"):
            clear_comment_dialog_state()
            st.rerun()
        return
    if not strategy_packages:
        st.info("暂无可用策略包，请先到“评论策略配置中心”创建。")
        if st.button("关闭", key=f"close_comment_dialog_no_pkg_{note_id}"):
            clear_comment_dialog_state()
            st.rerun()
        return

    comments_df = get_comments(note_id, limit=50)
    comment_candidates = comments_df.to_dict("records") if not comments_df.empty else []
    target_option_keys, target_option_map = build_comment_target_options(comment_candidates)

    default_mode = st.session_state.get("selected_comment_mode")
    if default_mode not in {"note", "reply"}:
        default_mode = "note"
    current_mode_label = st.radio(
        "评论方式",
        options=["整帖评论", "回复具体评论"],
        index=0 if default_mode == "note" else 1,
        horizontal=True,
        key=f"comment_mode_picker_{note_id}",
    )
    current_mode = "reply" if current_mode_label == "回复具体评论" else "note"
    st.session_state.selected_comment_mode = current_mode

    selected_target = None
    if current_mode == "reply":
        if not target_option_keys:
            st.warning("当前笔记暂无可回复的评论数据，请切换到“整帖评论”或重新抓取评论。")
        else:
            default_target_key = st.session_state.get("selected_comment_target_key")
            if default_target_key not in target_option_map:
                default_target_key = target_option_keys[0]
                st.session_state.selected_comment_target_key = default_target_key

            selected_target_key = st.selectbox(
                "选择要回复的评论",
                options=target_option_keys,
                index=target_option_keys.index(default_target_key),
                format_func=lambda x: format_comment_target_label(target_option_map[x]),
                key=f"reply_target_picker_{note_id}",
            )
            st.session_state.selected_comment_target_key = selected_target_key
            selected_target = dict(target_option_map[selected_target_key])
            st.session_state.selected_comment_target_id = selected_target.get("comment_id")
            st.session_state.selected_comment_target_user_id = selected_target.get("user_id")
            st.session_state.selected_comment_target_nickname = selected_target.get("nickname")
            st.session_state.selected_comment_target_preview = selected_target.get("preview")

            st.caption(f"当前将回复 @{selected_target.get('nickname') or '匿名用户'}")
            st.caption(f"目标评论：{selected_target.get('preview') or '（空评论）'}")
    else:
        st.caption("当前将直接在整篇笔记下发表评论。")

    if not xsec_token:
        st.warning("当前笔记缺少 xsec_token，无法真实发送评论。建议重新抓取该笔记后再试。")

    id_list = [str(t.get("id")) for t in templates]
    default_id = st.session_state.get("comment_template_selected_id")
    if default_id not in id_list:
        default_id = id_list[0]
        st.session_state.comment_template_selected_id = default_id

    selected_id = st.selectbox(
        "选择话术模板",
        options=id_list,
        index=id_list.index(default_id),
        format_func=lambda x: next(
            (
                f"{t.get('name', '-')}"
                f"（已用 {int(t.get('used_count', 0))} 次）"
                for t in templates if str(t.get("id")) == x
            ),
            x
        ),
        key=f"comment_template_picker_{note_id}",
    )
    st.session_state.comment_template_selected_id = selected_id
    selected_template = next(
        (t for t in templates if str(t.get("id")) == str(selected_id)),
        templates[0]
    )

    pkg_ids = [str(p.get("id")) for p in strategy_packages]
    default_pkg_id = st.session_state.get("comment_strategy_package_selected_id")
    if default_pkg_id not in pkg_ids:
        default_pkg_id = pkg_ids[0]
        st.session_state.comment_strategy_package_selected_id = default_pkg_id
    selected_pkg_id = st.selectbox(
        "选择策略包",
        options=pkg_ids,
        index=pkg_ids.index(default_pkg_id),
        format_func=lambda x: next(
            (
                f"{p.get('name', '-')}"
                f"（日上限 {int(p.get('daily_limit', 0))} / 小时上限 {int(p.get('hourly_limit', 0))}）"
                for p in strategy_packages if str(p.get("id")) == x
            ),
            x
        ),
        key=f"comment_strategy_package_picker_{note_id}",
    )
    st.session_state.comment_strategy_package_selected_id = selected_pkg_id
    selected_pkg = next(
        (p for p in strategy_packages if str(p.get("id")) == str(selected_pkg_id)),
        strategy_packages[0],
    )

    account_ids = [str(a.get("account_id")) for a in comment_accounts]
    default_account_id = st.session_state.get("comment_executor_account_selected_id")
    if default_account_id not in account_ids:
        default_account_id = account_ids[0]
        st.session_state.comment_executor_account_selected_id = default_account_id
    selected_account_id = st.selectbox(
        "选择评论账号（用于记录）",
        options=account_ids,
        index=account_ids.index(default_account_id),
        format_func=lambda x: next(
            (
                f"{a.get('account_name', '-')}" f"（ID: {a.get('account_id', '-')})"
                for a in comment_accounts if str(a.get("account_id")) == str(x)
            ),
            str(x),
        ),
        key=f"comment_executor_account_picker_{note_id}",
    )
    st.session_state.comment_executor_account_selected_id = selected_account_id
    selected_account = next(
        (a for a in comment_accounts if str(a.get("account_id")) == str(selected_account_id)),
        comment_accounts[0],
    )

    verifier_option_ids = ["__none__"] + [str(item.get("account_id")) for item in verifier_accounts]
    default_verifier = get_default_verifier_account(st.session_state.username)
    default_verifier_id = st.session_state.get("comment_verifier_account_selected_id")
    if default_verifier_id not in verifier_option_ids:
        default_verifier_id = str((default_verifier or {}).get("account_id") or "__none__")
        st.session_state.comment_verifier_account_selected_id = default_verifier_id
    selected_verifier_id = st.selectbox(
        "选择回溯验证账号（可选）",
        options=verifier_option_ids,
        index=verifier_option_ids.index(default_verifier_id),
        format_func=lambda x: (
            "暂不指定，后续在“执行中心”手动选择"
            if x == "__none__"
            else next(
                (
                    f"{item.get('account_name', '-')}" f"（ID: {item.get('account_id', '-')}）"
                    for item in verifier_accounts if str(item.get("account_id")) == str(x)
                ),
                str(x),
            )
        ),
        key=f"comment_verifier_account_picker_{note_id}",
    )
    st.session_state.comment_verifier_account_selected_id = selected_verifier_id
    selected_verifier = next(
        (item for item in verifier_accounts if str(item.get("account_id")) == str(selected_verifier_id)),
        None,
    )

    draft_key = f"comment_template_draft_{note_id}"
    if draft_key not in st.session_state:
        st.session_state[draft_key] = compose_template_preview(selected_template)
    elif st.session_state.get("comment_template_last_id") != selected_id:
        st.session_state[draft_key] = compose_template_preview(selected_template)
    st.session_state.comment_template_last_id = selected_id

    st.text_area(
        "评论内容预览 / 微调",
        key=draft_key,
        height=120,
        help="这里会真实调用评论服务发送评论，请确认内容后再提交。",
    )

    image_url = (selected_template.get("image_url") or "").strip()
    if image_url:
        st.caption("模板图片预览")
        st.image(image_url, width=260)

    tags = selected_template.get("tags") or []
    tags_text = " / ".join([str(t) for t in tags]) if tags else "-"
    st.caption(
        f"分类：{selected_template.get('category', '-')}"
        f"｜场景：{selected_template.get('scene', '-')}"
        f"｜标签：{tags_text}"
    )
    st.caption(
        f"当前策略包：{selected_pkg.get('name', '-')}"
        f"（{selected_pkg.get('active_start', '08:30')} - {selected_pkg.get('active_end', '23:00')}）"
    )
    st.caption(
        f"记录账号：{selected_account.get('account_name', '-')}"
        f"（ID: {selected_account.get('account_id', '-')})"
    )
    if using_runtime_comment_account:
        st.caption("未配置评论账号列表，发送时将直接使用 xiaohongshu-mcp 默认运行时登录态。")
    else:
        st.caption("发送时会按此账号ID切换 xiaohongshu-mcp 的独立运行时登录态，请先确保该账号已在执行中心完成登录。")
    if selected_verifier:
        st.caption(
            f"发送成功后会进入“待回溯”，默认使用验证账号：{selected_verifier.get('account_name', '-')}"
            f"（ID: {selected_verifier.get('account_id', '-')})"
        )
    else:
        st.caption("当前未绑定验证账号，发送成功后只会记录为“接口发送成功”，可稍后在执行中心手动回溯。")

    send_disabled = current_mode == "reply" and not selected_target
    c1, c2, c3 = st.columns([1, 1, 1.3])
    if c1.button("取消", key=f"cancel_template_dialog_{note_id}", use_container_width=True):
        clear_comment_dialog_state()
        st.rerun()
    enqueue_clicked = c2.button(
        "加入回复队列" if current_mode == "reply" else "加入评论队列",
        key=f"enqueue_template_dialog_{note_id}",
        type="primary",
        use_container_width=True,
        disabled=send_disabled,
    )
    immediate_clicked = c3.button(
        "忽略策略立即发送回复" if current_mode == "reply" else "忽略策略立即发送评论",
        key=f"use_template_dialog_{note_id}",
        use_container_width=True,
        disabled=send_disabled,
    )
    if enqueue_clicked or immediate_clicked:
        comment_content = str(st.session_state.get(draft_key, "")).strip()
        if not comment_content:
            st.error("评论内容不能为空。")
            return
        if not xsec_token:
            st.error("当前笔记缺少 xsec_token，无法发送评论。")
            return
        if current_mode == "reply" and not selected_target:
            st.error("请选择要回复的具体评论。")
            return

        schedule_result = compute_comment_schedule(
            st.session_state.username,
            now_dt=datetime.now(),
            strategy_override=_strategy_from_package(selected_pkg),
            strategy_package_id=selected_pkg_id,
        )
        suggested_dt = schedule_result["scheduled_dt"]
        scheduled_for = _now_str() if immediate_clicked else suggested_dt.strftime("%Y-%m-%d %H:%M:%S")
        job_data = build_comment_job_data(
            note=note,
            selected_template=selected_template,
            selected_pkg=selected_pkg,
            selected_account=selected_account,
            comment_content=comment_content,
            current_mode=current_mode,
            selected_target=selected_target,
            verifier_account=selected_verifier,
            schedule_result=schedule_result,
            scheduled_for=scheduled_for,
            force_status="queued",
        )
        job_id = create_comment_job(st.session_state.username, job_data)

        if enqueue_clicked:
            if schedule_result["delayed"]:
                st.toast(
                    f"已加入队列，策略建议执行时间：{suggested_dt.strftime('%Y-%m-%d %H:%M:%S')}。"
                )
            else:
                st.toast(
                    f"已加入队列，最早可执行时间：{suggested_dt.strftime('%Y-%m-%d %H:%M:%S')}。"
                )
            action_text = "回复" if current_mode == "reply" else "评论"
            st.success(f"{action_text}任务已入队，可前往“执行中心”查看和执行。")
            clear_comment_dialog_state()
            st.rerun()

        execution_results = process_due_comment_jobs(
            st.session_state.username,
            limit=1,
            specific_job_ids=[job_id],
        )
        result = execution_results[0] if execution_results else None
        if not result or not result.get("ok"):
            error_message = (result or {}).get("error") or "评论任务已创建，但立即发送失败。"
            st.error(f"评论发送失败：{error_message}")
            st.caption("失败记录已进入执行中心，可稍后重试。")
            return

        if schedule_result["delayed"]:
            st.toast(
                f"策略建议最早发送时间为 {suggested_dt.strftime('%Y-%m-%d %H:%M:%S')}，本次已忽略策略立即发送。"
            )
        else:
            st.toast(f"策略包[{selected_pkg.get('name', '-')}]允许立即执行，本次已发送。")

        action_text = "回复" if current_mode == "reply" else "评论"
        mcp_user = str((result.get("result") or {}).get("mcp_username") or "").strip() or "当前登录态"
        st.toast(f"{action_text}发送成功，实际执行账号：{mcp_user}")
        clear_comment_dialog_state()
        st.rerun()

def create_task(keyword, sort_type, max_count, start_date, end_date, account_name, filter_condition="", low_risk_mode=False):
    conn = get_tasks_connection()
    cursor = conn.execute(
        "INSERT INTO crawler_tasks (keyword, sort_type, max_count, start_date, end_date, status, account_name, dashboard_user, last_error, filter_condition, low_risk_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            keyword,
            sort_type,
            max_count,
            str(start_date),
            str(end_date),
            "Pending",
            account_name,
            st.session_state.username,
            None,
            str(filter_condition or "").strip(),
            1 if low_risk_mode else 0,
        )
    )
    conn.commit()
    conn.close()
    return int(cursor.lastrowid)

def get_tasks_for_user(username):
    conn = get_tasks_connection()
    df = pd.read_sql_query(
        "SELECT * FROM crawler_tasks WHERE dashboard_user = ? ORDER BY id DESC, created_at DESC",
        conn,
        params=(str(username),),
    )
    conn.close()
    return df

def get_tasks():
    return get_tasks_for_user(st.session_state.username)


def parse_task_created_at_ms(created_at):
    if not created_at:
        return 0
    try:
        # sqlite CURRENT_TIMESTAMP is UTC; interpret task timestamps in UTC to align
        # with xhs_note add_ts/last_modify_ts epoch values and avoid counting old records.
        dt_utc = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt_utc.timestamp() * 1000)
    except Exception:
        return 0

def normalize_task_pid(raw_pid):
    if raw_pid is None:
        return None
    try:
        if pd.isna(raw_pid):
            return None
    except Exception:
        pass
    text = str(raw_pid).strip()
    if not text or text.lower() in {"none", "nan"}:
        return None
    try:
        return int(float(text))
    except Exception:
        return None

def parse_bool_flag(raw_value, default=False):
    if raw_value is None:
        return default
    try:
        if pd.isna(raw_value):
            return default
    except Exception:
        pass
    text = str(raw_value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    try:
        return bool(int(float(text)))
    except Exception:
        return default


def get_task_crawled_count(task):
    conn = get_connection()
    try:
        keyword = str(task.get("keyword", "") or "")
        dashboard_user = str(task.get("dashboard_user", "") or "")
        account_name = str(task.get("account_name", "") or "")
        created_at_ms = parse_task_created_at_ms(task.get("created_at"))
        # Keep a short backward window to include records inserted around task start.
        min_ts = max(created_at_ms - 10_000, 0)

        if account_name and account_name != "默认 (扫码)":
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT note_id)
                FROM xhs_note
                WHERE dashboard_user = ?
                  AND source_keyword = ?
                  AND account_name = ?
                  AND COALESCE(add_ts, last_modify_ts, 0) >= ?
                """,
                (dashboard_user, keyword, account_name, min_ts)
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT note_id)
                FROM xhs_note
                WHERE dashboard_user = ?
                  AND source_keyword = ?
                  AND COALESCE(add_ts, last_modify_ts, 0) >= ?
                """,
                (dashboard_user, keyword, min_ts)
            ).fetchone()
        return int((row or [0])[0] or 0)
    except Exception:
        return 0
    finally:
        conn.close()

def get_task_error_guidance(error_message):
    err = str(error_message or "").strip()
    if not err:
        return ""
    if "浏览器启动失败" in err or "Crashpad" in err:
        return "建议：这是本机浏览器权限问题，不是账号问题。先完全退出 Chrome 后重试；若仍失败，请在 macOS「系统设置-隐私与安全性-完全磁盘访问权限」中给终端/IDE授权后重试。"
    if "账号风控" in err:
        return "建议：这类报错通常是账号/API权限风控而非IP封禁。请更新账号 Cookie，并先在浏览器人工访问小红书完成一次验证后再重试。"
    if "Cookie" in err and ("失效" in err or "无效" in err):
        return "建议：到“账号管理”重新保存最新 Cookie，再重新启动任务。"
    if "网络/IP限制" in err:
        return "建议：切换网络出口（如手机热点）或代理后重试；若仅单账号失败，优先排查账号风控。"
    if "接口连续重试后仍失败" in err:
        return "建议：先更换关键词和账号小规模验证，确认可跑通后再放量。"
    return ""

def classify_task_log_error(text):
    lowered = str(text or "").lower()
    has_qrcode_success = (
        "login_by_qrcode" in lowered
        and ("login status confirmed" in lowered or "login successful" in lowered)
    )

    account_risk_signals = (
        "没有权限访问",
        "账号没有权限",
        "账号异常",
        "account abnormal",
        "risk control",
        "风控",
    )
    captcha_signals = (
        "captcha appeared",
        "请通过验证",
        "滑块",
        "verify",
    )
    cookie_context_signals = (
        "cookie",
        "invalid",
        "expired",
        "401",
        "unauthorized",
        "forbidden",
    )
    network_ip_signals = (
        "timed out",
        "timeout",
        "connection reset",
        "connection aborted",
        "name or service not known",
        "temporary failure in name resolution",
        "max retries exceeded",
        "proxyerror",
        "err_proxy_connection_failed",
        "err_tunnel_connection_failed",
        "err_connection_timed_out",
        "readtimeout",
        "connecttimeout",
        "network is unreachable",
        "connection refused",
    )
    browser_launch_signals = (
        "launch_persistent_context timeout after",
        "target page, context or browser has been closed",
        "begin create browser context",
    )
    crashpad_signals = (
        "operation not permitted",
        "crashpad",
        "xattr.cc",
    )

    if any(signal in lowered for signal in account_risk_signals):
        return "账号风控：当前登录账号没有权限访问该关键词/内容。请更换账号Cookie或先人工验证后重试。"
    if any(signal in lowered for signal in browser_launch_signals) and any(signal in lowered for signal in crashpad_signals):
        return "浏览器启动失败：系统阻止了 Chrome 的 Crashpad 权限访问。请先完全退出 Chrome 后重试；若仍失败，需给终端/IDE开启系统磁盘访问权限。"
    if "nosuchoption" in lowered or "no such option" in lowered:
        return "任务配置错误：启动参数不被当前爬虫版本支持。请更新面板参数后重试。"
    if any(signal in lowered for signal in captcha_signals):
        return "账号风控：触发验证码校验，请先在浏览器人工验证后再重试任务。"
    if "retryerror" in lowered and "datafetcherror" in lowered:
        if any(signal in lowered for signal in network_ip_signals):
            return "疑似网络/IP限制：请求多次超时或连接失败。请切换网络出口或代理后重试。"
        return "任务失败：接口连续重试后仍失败，请检查账号权限、Cookie有效性或稍后重试。"
    if any(signal in lowered for signal in network_ip_signals):
        return "疑似网络/IP限制：请求连接异常。请切换网络出口或代理后重试。"
    if "cookie" in lowered and any(signal in lowered for signal in ("invalid", "expired", "失效", "无效", "unauthorized", "forbidden", "401")):
        return "账号异常：Cookie疑似失效，请到“账号管理”重新保存后再重试。"
    if (
        not has_qrcode_success
        and "login state result: false" in lowered
        and any(signal in lowered for signal in cookie_context_signals)
    ):
        return "账号异常：登录态校验失败，Cookie可能已失效，请重新保存后再试。"
    return None

def extract_task_error(task_id):
    log_path = os.path.join("logs", f"task_{task_id}.log")
    if not os.path.exists(log_path):
        return None
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except Exception:
        return None
    if not text:
        return None

    classified_error = classify_task_log_error(text)
    if classified_error:
        return classified_error

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    for line in reversed(lines):
        if re.search(r"(Error|Exception):", line):
            return line[:500]

    for line in reversed(lines):
        if " ERROR " in line or line.startswith("ERROR"):
            return line[:500]
    return None


def task_finished_successfully(task_id):
    log_path = os.path.join("logs", f"task_{task_id}.log")
    if not os.path.exists(log_path):
        return False
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except Exception:
        return False
    if "Xhs Crawler finished" not in text or "Traceback" in text:
        return False
    # A finished marker can still appear when crawler handles an error path internally.
    return extract_task_error(task_id) is None


def is_process_zombie(pid):
    try:
        result = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(int(pid))],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return False
    if result.returncode != 0:
        return False
    stat = (result.stdout or "").strip()
    return "Z" in stat

def has_available_login_state(account_name):
    if account_name and account_name != "默认 (扫码)":
        accounts = load_accounts()
        account = next((acc for acc in accounts if acc.get("name") == account_name), None)
        if not account or not account.get("cookie"):
            return False, "所选账号缺少有效 Cookie，请到“账号管理”重新保存后再创建任务。"
        return True, None

    # 默认(扫码)模式允许直接创建/启动任务，首次运行时可在浏览器完成扫码登录。
    return True, None

def has_saved_default_login_state():
    profile_dir = os.path.join(os.getcwd(), "browser_data", config.USER_DATA_DIR % "xhs")
    try:
        return os.path.isdir(profile_dir) and any(os.scandir(profile_dir))
    except Exception:
        return False

def get_default_scan_login_warning(account_name):
    if account_name and account_name != "默认 (扫码)":
        return None
    if has_saved_default_login_state():
        return None
    return "当前使用“默认(扫码)”且尚未检测到已保存登录态：首次启动可能需要在浏览器扫码/过验证。完成后会自动复用登录态。"

def reconcile_running_tasks():
    """Mark stale running tasks as Error when process no longer exists."""
    conn = get_tasks_connection()
    try:
        rows = conn.execute(
            "SELECT id, pid FROM crawler_tasks WHERE dashboard_user = ? AND status = 'Running'",
            (st.session_state.username,)
        ).fetchall()
        for task_id, pid in rows:
            normalized_pid = normalize_task_pid(pid)
            if normalized_pid is None:
                conn.execute(
                    "UPDATE crawler_tasks SET status = ?, last_error = ?, pid = NULL WHERE id = ?",
                    ("Error", "任务进程 PID 无效，请重新启动任务。", task_id)
                )
                continue
            fatal_error = extract_task_error(task_id)
            if fatal_error:
                try:
                    os.kill(normalized_pid, signal.SIGTERM)
                except Exception:
                    pass
                conn.execute(
                    "UPDATE crawler_tasks SET status = ?, last_error = ?, pid = NULL WHERE id = ?",
                    ("Error", fatal_error[:500], task_id)
                )
                continue
            try:
                os.kill(normalized_pid, 0)
                if is_process_zombie(normalized_pid):
                    if task_finished_successfully(task_id):
                        conn.execute(
                            "UPDATE crawler_tasks SET status = ?, last_error = NULL, pid = NULL WHERE id = ?",
                            ("Completed", task_id)
                        )
                    else:
                        error_message = fatal_error or "任务进程已异常退出，请查看日志。"
                        conn.execute(
                            "UPDATE crawler_tasks SET status = ?, last_error = ?, pid = NULL WHERE id = ?",
                            ("Error", error_message, task_id)
                        )
            except Exception:
                if task_finished_successfully(task_id):
                    conn.execute(
                        "UPDATE crawler_tasks SET status = ?, last_error = NULL, pid = NULL WHERE id = ?",
                        ("Completed", task_id)
                    )
                else:
                    error_message = fatal_error or "任务进程已退出，请查看日志。"
                    conn.execute(
                        "UPDATE crawler_tasks SET status = ?, last_error = ?, pid = NULL WHERE id = ?",
                        ("Error", error_message, task_id)
                    )
        conn.commit()
    finally:
        conn.close()


def reconcile_completed_tasks():
    """Downgrade pseudo-completed tasks to Error when logs contain fatal errors."""
    conn = get_tasks_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, keyword, sort_type, max_count, start_date, end_date, account_name, dashboard_user, created_at
            FROM crawler_tasks
            WHERE dashboard_user = ? AND status = 'Completed'
            ORDER BY created_at DESC
            LIMIT 100
            """,
            (st.session_state.username,),
        ).fetchall()

        for row in rows:
            task = {
                "id": row[0],
                "keyword": row[1],
                "sort_type": row[2],
                "max_count": row[3],
                "start_date": row[4],
                "end_date": row[5],
                "account_name": row[6],
                "dashboard_user": row[7],
                "created_at": row[8],
            }
            fatal_error = extract_task_error(task["id"])
            if not fatal_error:
                continue
            crawled_count = get_task_crawled_count(task)
            if crawled_count == 0:
                conn.execute(
                    "UPDATE crawler_tasks SET status = ?, last_error = ?, pid = NULL WHERE id = ?",
                    ("Error", fatal_error[:500], task["id"]),
                )
                if str(task.get("dashboard_user", "")).strip():
                    append_operation_exception(
                        username=str(task.get("dashboard_user", "")),
                        source_module="crawl",
                        severity="critical",
                        error_message_raw=str(fatal_error or ""),
                        error_message_normalized=f"爬取任务失败：{fatal_error}",
                        task_id=int(task["id"]),
                        account_id=str(task.get("account_name") or ""),
                        error_code="crawler_task_fatal",
                    )
        conn.commit()
    finally:
        conn.close()

def update_task_status(task_id, status, pid=None, error_message=None):
    conn = get_tasks_connection()
    task_row = None
    if str(status) == "Error":
        cursor = conn.execute(
            "SELECT id, keyword, account_name, dashboard_user FROM crawler_tasks WHERE id = ? LIMIT 1",
            (task_id,),
        )
        task_row = cursor.fetchone()
    if pid is not None:
        conn.execute(
            "UPDATE crawler_tasks SET status = ?, pid = ?, last_error = NULL WHERE id = ?",
            (status, pid, task_id)
        )
    elif error_message is not None:
        conn.execute(
            "UPDATE crawler_tasks SET status = ?, last_error = ?, pid = NULL WHERE id = ?",
            (status, error_message[:500], task_id)
        )
    elif status in {"Error", "Completed", "Stopped"}:
        conn.execute(
            "UPDATE crawler_tasks SET status = ?, pid = NULL WHERE id = ?",
            (status, task_id)
        )
    else:
        conn.execute(
            "UPDATE crawler_tasks SET status = ?, last_error = NULL WHERE id = ?",
            (status, task_id)
        )
    conn.commit()
    conn.close()
    if str(status) == "Error" and error_message is not None and task_row:
        username = str(task_row[3] or "").strip()
        if username:
            append_operation_exception(
                username=username,
                source_module="crawl",
                severity="critical",
                error_message_raw=str(error_message or ""),
                error_message_normalized=f"爬取任务失败：{error_message}",
                task_id=int(task_id),
                account_id=str(task_row[2] or ""),
                error_code="crawler_task_error",
            )

def build_crawler_env():
    env = os.environ.copy()
    env["PLAYWRIGHT_BROWSERS_PATH"] = os.path.abspath(".playwright")
    os.makedirs(env["PLAYWRIGHT_BROWSERS_PATH"], exist_ok=True)

    runtime_home = os.path.abspath(".runtime_home")
    runtime_tmp = os.path.join(runtime_home, "tmp")
    runtime_state = os.path.join(runtime_home, ".state")
    env["HOME"] = runtime_home
    env["TMPDIR"] = runtime_tmp
    env["XDG_CONFIG_HOME"] = os.path.join(runtime_home, ".config")
    env["XDG_CACHE_HOME"] = os.path.join(runtime_home, ".cache")
    env["XDG_STATE_HOME"] = runtime_state
    env["PLAYWRIGHT_DISABLE_CRASH_REPORTER"] = "1"
    os.makedirs(env["HOME"], exist_ok=True)
    os.makedirs(env["TMPDIR"], exist_ok=True)
    os.makedirs(env["XDG_CONFIG_HOME"], exist_ok=True)
    os.makedirs(env["XDG_CACHE_HOME"], exist_ok=True)
    os.makedirs(env["XDG_STATE_HOME"], exist_ok=True)

    runtime_llm_settings = load_llm_runtime_settings_for_user(st.session_state.username)
    runtime_api_key = str(runtime_llm_settings.get("api_key", "")).strip()
    runtime_base_url = str(runtime_llm_settings.get("base_url", "")).strip()
    runtime_model = str(runtime_llm_settings.get("model", "")).strip()
    if runtime_api_key:
        env["XHS_LIST_FILTER_API_KEY"] = runtime_api_key
        env["AGENT_REPORT_API_KEY"] = runtime_api_key
    if runtime_base_url:
        env["XHS_LIST_FILTER_API_BASE"] = runtime_base_url
        env["AGENT_REPORT_API_BASE"] = runtime_base_url
    if runtime_model:
        env["XHS_LIST_FILTER_MODEL"] = runtime_model
        env["AGENT_REPORT_MODEL"] = runtime_model
    return env

def get_runtime_python():
    venv_python = os.path.join(os.getcwd(), "venv", "bin", "python")
    if os.path.exists(venv_python):
        return venv_python
    return sys.executable

def preflight_browser_check():
    """Quickly verify Chromium can be launched before task start."""
    crashpad_dir = os.path.join(os.getcwd(), ".runtime_home", "crashpad")
    runtime_env = build_crawler_env()
    timeout_raw = str(os.getenv("BROWSER_PREFLIGHT_TIMEOUT_SEC", "60")).strip()
    try:
        timeout_sec = int(timeout_raw)
    except ValueError:
        timeout_sec = 60
    timeout_sec = max(10, min(timeout_sec, 300))
    preflight_log_path = os.path.join("logs", "browser_preflight.log")

    def _last_non_empty_lines(text, max_lines=8):
        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
        if not lines:
            return ""
        return "\n".join(lines[-max_lines:])

    def _append_preflight_log(title, body):
        os.makedirs("logs", exist_ok=True)
        with open(preflight_log_path, "a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {title}\n")
            f.write((body or "(no detail)") + "\n")

    @contextlib.contextmanager
    def _temporary_environ(overrides):
        old_values = {}
        try:
            for key, value in overrides.items():
                old_values[key] = os.environ.get(key)
                os.environ[key] = str(value)
            yield
        finally:
            for key, old_value in old_values.items():
                if old_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old_value

    async def _launch_browser_once():
        from playwright.async_api import async_playwright

        args = [
            "--noerrdialogs",
            "--disable-crashpad-for-testing",
            "--disable-crash-reporter",
            "--disable-gpu",
            f"--crash-dumps-dir={crashpad_dir}",
        ]
        fallback_paths = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ]
        headless_modes = [True]
        if not bool(getattr(config, "HEADLESS", True)):
            # If production run uses headed mode, allow a headed preflight retry to avoid false negatives.
            headless_modes.append(False)
        async with async_playwright() as p:
            last_error = None
            for headless_mode in headless_modes:
                try:
                    browser = await p.chromium.launch(headless=headless_mode, args=args, env=runtime_env)
                    await browser.close()
                    return
                except Exception as first_error:
                    # Some macOS environments deny Chromium crashpad xattr access.
                    # Retry with installed stable channels as a fallback.
                    last_error = first_error
                for channel in ("chrome", "msedge"):
                    try:
                        browser = await p.chromium.launch(
                            headless=headless_mode,
                            args=args,
                            channel=channel,
                            env=runtime_env,
                        )
                        await browser.close()
                        return
                    except Exception as channel_error:
                        last_error = channel_error
                for path in fallback_paths:
                    if not os.path.isfile(path):
                        continue
                    try:
                        browser = await p.chromium.launch(
                            headless=headless_mode,
                            args=args,
                            executable_path=path,
                            env=runtime_env,
                        )
                        await browser.close()
                        return
                    except Exception as path_error:
                        last_error = path_error
            raise last_error

    def _run_launch_with_timeout():
        env_overrides = {
            key: runtime_env.get(key)
            for key in (
                "PLAYWRIGHT_BROWSERS_PATH",
                "HOME",
                "TMPDIR",
                "XDG_CONFIG_HOME",
                "XDG_CACHE_HOME",
                "XDG_STATE_HOME",
                "PLAYWRIGHT_DISABLE_CRASH_REPORTER",
            )
            if str(runtime_env.get(key, "")).strip()
        }

        def _runner():
            with _temporary_environ(env_overrides):
                asyncio.run(_launch_browser_once())

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(_runner)
        try:
            return future.result(timeout=timeout_sec)
        finally:
            # Do not block the Streamlit request thread on hung browser startup.
            if future.done():
                executor.shutdown(wait=True, cancel_futures=True)
            else:
                future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)

    try:
        _run_launch_with_timeout()
    except concurrent.futures.TimeoutError:
        _append_preflight_log(
            "BROWSER PREFLIGHT TIMEOUT",
            (
                f"timeout_sec={timeout_sec}\n"
                f"python={get_runtime_python()}\n"
                "runner=direct_asyncio_thread\n"
                "partial_output:\n(empty)"
            ),
        )
        return False, f"浏览器自检超时（{timeout_sec}秒），请稍后重试。详情见 {preflight_log_path}"
    except Exception as e:
        _append_preflight_log(
            "BROWSER PREFLIGHT EXCEPTION",
            (
                f"python={get_runtime_python()}\n"
                "runner=direct_asyncio_thread\n"
                f"error={e}\n"
                f"error_type={type(e).__name__}"
            ),
        )
        message = str(e)
        if "Operation not permitted" in message and "Crashpad" in message:
            return False, "浏览器自检失败：系统阻止了浏览器 Crashpad 目录写入。"
        if "ModuleNotFoundError" in message:
            return False, f"浏览器自检失败：运行环境缺少依赖。{message}"
        if "TargetClosedError" in message:
            return False, "浏览器自检失败：浏览器进程异常退出。"
        return False, f"浏览器自检失败，任务未启动。{message}。详情见 {preflight_log_path}"

    return True, None

def run_crawler(task_id, keyword, sort_type, max_count, account_name, filter_condition="", low_risk_mode=False):
    # Map sort type to CLI argument value
    sort_map = {
        "综合排序": "general",
        "最热": "popularity_descending",
        "最新": "time_descending"
    }
    sort_arg = sort_map.get(sort_type, "general")
    
    os.makedirs("logs", exist_ok=True)
    log_file = open(f"logs/task_{task_id}.log", "w")
    
    cmd = [
        get_runtime_python(), "main.py",
        "--platform", "xhs",
        "--crawler-type", "search",
        "--keywords", keyword,
        "--sort-type", sort_arg,
        "--save-data-option", "sqlite",
        "--crawler-max-notes-count", str(max_count),
        "--dashboard-user", st.session_state.username
    ]
    normalized_filter_condition = str(filter_condition or "").strip()
    if normalized_filter_condition.lower() == "nan":
        normalized_filter_condition = ""
    if normalized_filter_condition:
        cmd.extend(["--list-filter-condition", normalized_filter_condition])
    if low_risk_mode:
        # Conservative profile: reduce request volume and pace down to lower risk.
        cmd.extend([
            "--get-comment", "false",
            "--max-comments-count-singlenotes", "1",
            "--max-concurrency-num", "1",
        ])
    
    # Handle account selection
    if account_name and account_name != "默认 (扫码)":
        accounts = load_accounts()
        account = next((acc for acc in accounts if acc['name'] == account_name), None)
        if account and account.get('cookie'):
            cmd.extend(["--lt", "cookie", "--cookies", account['cookie']])
            cmd.extend(["--account-name", account_name])
        else:
            cmd.extend(["--lt", "qrcode"])
    else:
        cmd.extend(["--lt", "qrcode"])
    
    process = subprocess.Popen(cmd, stdout=log_file, stderr=log_file, env=build_crawler_env())
    update_task_status(task_id, "Running", process.pid)
    return process.pid

# Main UI
APP_TITLE = "📕 小红书电子员工工作台"

# Define users
USERS = {
    "18018598859": "qweasd123",
    "18016277417": "qweasd123"
}

def check_login(username, password):
    return USERS.get(username) == password

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""

# Auto-login via Query Params
if not st.session_state.logged_in:
    query_params = st.query_params
    token = query_params.get("token")
    if token:
        try:
            # Simple base64 decode
            decoded_user = base64.b64decode(token.encode()).decode()
            if decoded_user in USERS:
                st.session_state.logged_in = True
                st.session_state.username = decoded_user
                st.toast(f"欢迎回来，{decoded_user}")
        except:
            pass

if not st.session_state.logged_in:
    st.title(APP_TITLE)
    st.header("登录")
    with st.form("login_form"):
        username = st.text_input("账号")
        password = st.text_input("密码", type="password")
        submitted = st.form_submit_button("登录")
        if submitted:
            if check_login(username, password):
                st.session_state.logged_in = True
                st.session_state.username = username
                # Set token in URL
                token = base64.b64encode(username.encode()).decode()
                st.query_params["token"] = token
                st.success("登录成功！")
                st.rerun()
            else:
                st.error("账号或密码错误")
    st.stop()  # Stop execution if not logged in

# Logout button in sidebar
st.sidebar.write(f"当前用户: {st.session_state.username}")
st.sidebar.caption("第一次使用建议先看「电子员工使用手册」，先创建电子员工，再给它配资源。")
runtime_llm_settings = load_llm_runtime_settings_for_user(st.session_state.username)
runtime_api_key = str(runtime_llm_settings.get("api_key", "")).strip()
runtime_base_url = str(runtime_llm_settings.get("base_url", "")).strip()
runtime_model = str(runtime_llm_settings.get("model", "")).strip()
with st.sidebar.expander("AI API Key 设置", expanded=False):
    st.caption("用于老板看板（KPI/日报）和爬取列表 AI 粗筛。")
    st.caption("当前状态：" + ("已配置" if runtime_api_key else "未配置"))
    if runtime_api_key:
        st.caption(f"当前密钥：{_mask_secret(runtime_api_key)}")
    current_base_url = runtime_base_url or str(
        getattr(config, "AGENT_REPORT_API_BASE", "")
        or getattr(config, "LIST_FILTER_API_BASE", "https://api.openai.com/v1")
    ).strip()
    current_model = runtime_model or str(
        getattr(config, "AGENT_REPORT_MODEL", "")
        or getattr(config, "LIST_FILTER_MODEL", "gpt-4o-mini")
    ).strip()
    st.caption(f"当前接口：{current_base_url or '未配置'}")
    st.caption(f"当前模型：{current_model or '未配置'}")
    with st.form("sidebar_ai_api_key_form"):
        api_key_input = st.text_input(
            "API Key",
            value="",
            type="password",
            placeholder="例如：sk-...",
            help="只在当前系统保存，不会展示明文。",
        )
        base_url_input = st.text_input(
            "Base URL",
            value=current_base_url,
            placeholder="例如：https://ai-platform-test.zhenguanyu.com/litellm/v1",
            help="OpenAI 兼容接口地址，不含 /chat/completions 后缀。",
        )
        model_input = st.text_input(
            "模型名称",
            value=current_model,
            placeholder="例如：deepseek-v3.2",
            help="填写比赛平台提供的模型标识。",
        )
        op1, op2 = st.columns(2)
        save_api_key_clicked = op1.form_submit_button("保存 Key", type="primary", use_container_width=True)
        clear_api_key_clicked = op2.form_submit_button("清空 Key", use_container_width=True)
        if save_api_key_clicked:
            if not api_key_input.strip():
                st.error("请输入有效的 API Key。")
            elif not _normalize_openai_base_url(base_url_input):
                st.error("请输入有效的 Base URL。")
            elif not model_input.strip():
                st.error("请输入有效的模型名称。")
            else:
                save_llm_runtime_settings_for_user(
                    st.session_state.username,
                    {
                        "api_key": api_key_input.strip(),
                        "base_url": _normalize_openai_base_url(base_url_input),
                        "model": model_input.strip(),
                    },
                )
                st.success("API Key 已保存并立即生效。")
                st.rerun()
        if clear_api_key_clicked:
            save_llm_runtime_settings_for_user(
                st.session_state.username,
                {"api_key": ""},
            )
            st.success("API Key 已清空。")
            st.rerun()
if st.sidebar.button("退出登录"):
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.query_params.clear()
    st.rerun()

if 'view_keyword' not in st.session_state:
    st.session_state.view_keyword = ""
if "view_source_keyword" not in st.session_state:
    st.session_state.view_source_keyword = ""
if "selected_comment_note_id" not in st.session_state:
    st.session_state.selected_comment_note_id = None
if "selected_comment_mode" not in st.session_state:
    st.session_state.selected_comment_mode = "note"
if "selected_comment_target_key" not in st.session_state:
    st.session_state.selected_comment_target_key = None
if "selected_comment_target_id" not in st.session_state:
    st.session_state.selected_comment_target_id = None
if "selected_comment_target_user_id" not in st.session_state:
    st.session_state.selected_comment_target_user_id = None
if "selected_comment_target_nickname" not in st.session_state:
    st.session_state.selected_comment_target_nickname = ""
if "selected_comment_target_preview" not in st.session_state:
    st.session_state.selected_comment_target_preview = ""
if "comment_template_selected_id" not in st.session_state:
    st.session_state.comment_template_selected_id = None
if "comment_template_last_id" not in st.session_state:
    st.session_state.comment_template_last_id = None
if "comment_strategy_package_selected_id" not in st.session_state:
    st.session_state.comment_strategy_package_selected_id = None
if "comment_executor_account_selected_id" not in st.session_state:
    st.session_state.comment_executor_account_selected_id = None
if "comment_verifier_account_selected_id" not in st.session_state:
    st.session_state.comment_verifier_account_selected_id = "__none__"
if "comment_execution_auto_run" not in st.session_state:
    # Default off to prevent entering Execution Center from triggering
    # background comment dispatch and repeated login popups.
    st.session_state.comment_execution_auto_run = False
if "employee_profile_selected_id" not in st.session_state:
    st.session_state.employee_profile_selected_id = None
if "employee_prompt_selected_id" not in st.session_state:
    st.session_state.employee_prompt_selected_id = None
if "boss_dashboard_report_date" not in st.session_state:
    st.session_state.boss_dashboard_report_date = datetime.now().date()
if "boss_dashboard_only_today" not in st.session_state:
    st.session_state.boss_dashboard_only_today = True
if "strategy_time_range" not in st.session_state:
    st.session_state.strategy_time_range = (datetime.strptime("08:30", "%H:%M").time(), datetime.strptime("23:00", "%H:%M").time())

page_groups = {
    "先看这里": ["电子员工使用手册"],
    "电子员工": ["员工管理", "老板看板"],
    "资源准备": ["爬取账号", "评论账号", "话术管理", "评论策略"],
    "执行工作": ["笔记收录", "笔记展示", "执行中心"],
}
page_options = [page for pages in page_groups.values() for page in pages]
default_dashboard_page = "笔记收录" if "笔记收录" in page_options else page_options[0]
resolved_dashboard_page = resolve_dashboard_page(page_options, default_dashboard_page)
if st.session_state.get("dashboard_page") != resolved_dashboard_page:
    st.session_state.dashboard_page = resolved_dashboard_page
sync_dashboard_page_query(st.session_state.dashboard_page)

for group_name, pages in page_groups.items():
    st.sidebar.markdown(f"### {group_name}")
    for page in pages:
        button_type = "primary" if st.session_state.dashboard_page == page else "secondary"
        if st.sidebar.button(
            page,
            key=f"dashboard_nav_{page}",
            type=button_type,
            use_container_width=True,
        ):
            switch_dashboard_page(page)

current_page = st.session_state.dashboard_page

if current_page != "电子员工使用手册":
    st.title(APP_TITLE)

if current_page == "电子员工使用手册":
    render_beginner_guide_page()

if current_page == "话术管理":
    st.header("话术管理")
    st.caption("创建和管理评论话术模板（支持正文 + 表情 + 图片链接），供“笔记展示”页的评论按钮弹窗选择。")

    with st.form("create_comment_template_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            tpl_name = st.text_input("模板名称 *", placeholder="例如：私聊引导-穿搭")
            tpl_category = st.selectbox("模板分类", ["种草型", "截流型", "问询型", "通用型"])
            tpl_scene = st.text_input("适用场景", placeholder="例如：穿搭、美妆")
            tpl_status = st.selectbox("模板状态", ["active", "inactive"], format_func=lambda x: "启用" if x == "active" else "停用")
        with c2:
            tpl_emoji = st.text_input("文本表情", placeholder="例如：✨🔥")
            tpl_image_url = st.text_input("图片链接", placeholder="可选，http(s)://...")
            tpl_tags = st.text_input("标签", placeholder="多个标签用逗号分隔，例如：引流,私聊,穿搭")
        tpl_content = st.text_area("模板正文 *", placeholder="输入评论模板正文...")

        create_tpl = st.form_submit_button("新增话术模板", type="primary")
        if create_tpl:
            if not tpl_name.strip() or not tpl_content.strip():
                st.error("请填写模板名称和模板正文。")
            else:
                templates = load_comment_templates(st.session_state.username)
                now = _now_str()
                templates.insert(
                    0,
                    {
                        "id": int(time.time() * 1000),
                        "dashboard_user": st.session_state.username,
                        "name": tpl_name.strip(),
                        "category": tpl_category,
                        "scene": tpl_scene.strip() or "通用场景",
                        "content": tpl_content.strip(),
                        "emoji": tpl_emoji.strip(),
                        "image_url": tpl_image_url.strip(),
                        "tags": [x.strip() for x in tpl_tags.split(",") if x.strip()],
                        "status": tpl_status,
                        "used_count": 0,
                        "created_at": now,
                        "updated_at": now,
                        "last_used_at": "-",
                    },
                )
                save_comment_templates_for_user(st.session_state.username, templates)
                st.success("模板创建成功。")
                st.rerun()

    st.markdown("---")
    st.subheader("模板列表")

    templates = load_comment_templates(st.session_state.username)
    if not templates:
        st.info("暂无模板，请先创建。")
    else:
        table_rows = []
        for item in templates:
            tags = item.get("tags") or []
            table_rows.append(
                {
                    "模板名称": item.get("name", "-"),
                    "模板内容（正文/图片/表情）": compose_template_preview(item),
                    "模板使用次数": int(item.get("used_count", 0)),
                    "模板创建时间": item.get("created_at", "-"),
                    "分类": item.get("category", "-"),
                    "适用场景": item.get("scene", "-"),
                    "状态": "启用" if item.get("status") == "active" else "停用",
                    "最近使用": item.get("last_used_at", "-"),
                    "标签": " / ".join(tags) if tags else "-",
                }
            )
        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

        template_id_options = [str(item.get("id")) for item in templates]
        delete_template_ids = st.multiselect(
            "选择要删除的话术模板（可多选）",
            options=template_id_options,
            format_func=lambda x: next(
                (
                    f"{t.get('name', '-')}" f"（ID: {t.get('id', '-')}）"
                    for t in templates if str(t.get("id")) == str(x)
                ),
                str(x),
            ),
        )
        if st.button("删除选中模板", type="secondary", disabled=not delete_template_ids):
            remained_templates = [
                t for t in templates
                if str(t.get("id")) not in {str(i) for i in delete_template_ids}
            ]
            save_comment_templates_for_user(st.session_state.username, remained_templates)
            if str(st.session_state.get("comment_template_selected_id")) in {str(i) for i in delete_template_ids}:
                st.session_state.comment_template_selected_id = None
            st.success(f"已删除 {len(delete_template_ids)} 个话术模板。")
            st.rerun()

if current_page == "评论策略":
    st.header("评论策略配置中心")
    st.caption("把风控参数打包为多个“策略包”，评论弹窗可按笔记选择任意策略包。")

    strategy_packages = load_comment_strategy_packages(st.session_state.username)
    if not strategy_packages:
        strategy_packages = [_default_comment_strategy_package(st.session_state.username)]
        save_comment_strategy_packages_for_user(st.session_state.username, strategy_packages)

    pkg_ids = [str(p.get("id")) for p in strategy_packages]
    selected_pkg_id = st.session_state.get("comment_strategy_package_selected_id")
    if selected_pkg_id not in pkg_ids:
        selected_pkg_id = pkg_ids[0]
        st.session_state.comment_strategy_package_selected_id = selected_pkg_id
    selected_pkg = next(
        (p for p in strategy_packages if str(p.get("id")) == str(selected_pkg_id)),
        strategy_packages[0],
    )

    st.subheader("策略包管理")
    with st.form("create_strategy_package_form", clear_on_submit=True):
        n1, n2 = st.columns(2)
        with n1:
            new_pkg_name = st.text_input("新策略包名称 *", placeholder="例如：稳健白天包")
        with n2:
            new_pkg_desc = st.text_input("策略包说明", placeholder="例如：低频、白天执行")
        create_pkg = st.form_submit_button("新增策略包")
        if create_pkg:
            if not new_pkg_name.strip():
                st.error("请填写策略包名称。")
            else:
                base = _default_comment_strategy()
                new_pkg = {
                    "id": f"pkg_{int(time.time() * 1000)}",
                    "dashboard_user": st.session_state.username,
                    "name": new_pkg_name.strip(),
                    "description": new_pkg_desc.strip(),
                    "daily_limit": int(base["daily_limit"]),
                    "hourly_limit": int(base["hourly_limit"]),
                    "jitter_min_sec": int(base["jitter_min_sec"]),
                    "jitter_max_sec": int(base["jitter_max_sec"]),
                    "active_start": str(base["active_start"]),
                    "active_end": str(base["active_end"]),
                    "updated_at": _now_str(),
                }
                strategy_packages.insert(0, new_pkg)
                save_comment_strategy_packages_for_user(st.session_state.username, strategy_packages)
                st.session_state.comment_strategy_package_selected_id = new_pkg["id"]
                st.success("策略包创建成功。")
                st.rerun()

    selected_pkg_id = st.selectbox(
        "选择要编辑的策略包",
        options=pkg_ids,
        index=pkg_ids.index(st.session_state.comment_strategy_package_selected_id),
        format_func=lambda x: next(
            (f"{p.get('name', '-')}" for p in strategy_packages if str(p.get("id")) == x),
            x,
        ),
    )
    st.session_state.comment_strategy_package_selected_id = selected_pkg_id
    selected_pkg = next(
        (p for p in strategy_packages if str(p.get("id")) == str(selected_pkg_id)),
        strategy_packages[0],
    )

    start_h, start_m = _parse_hhmm(selected_pkg.get("active_start", "08:30"), 8, 30)
    end_h, end_m = _parse_hhmm(selected_pkg.get("active_end", "23:00"), 23, 0)
    default_time_range = (
        datetime.strptime(f"{start_h:02d}:{start_m:02d}", "%H:%M").time(),
        datetime.strptime(f"{end_h:02d}:{end_m:02d}", "%H:%M").time(),
    )

    st.subheader("1) 基础配额模块")
    q1, q2 = st.columns(2)
    with q1:
        pkg_name = st.text_input("策略包名称", value=str(selected_pkg.get("name", "")))
        daily_limit = st.number_input(
            "单日评论上限（每账号）",
            min_value=1,
            max_value=5000,
            value=int(selected_pkg.get("daily_limit", 120)),
            step=1,
        )
    with q2:
        pkg_desc = st.text_input("策略包说明", value=str(selected_pkg.get("description", "")))
        hourly_limit = st.number_input(
            "瞬时频率限制（每小时最高评论数）",
            min_value=1,
            max_value=1000,
            value=int(selected_pkg.get("hourly_limit", 20)),
            step=1,
        )

    st.subheader("2) 拟人随机化模块")
    j1, j2 = st.columns(2)
    with j1:
        jitter_min_sec = st.number_input(
            "非等比随机间隔最小值（秒）",
            min_value=0,
            max_value=3600,
            value=int(selected_pkg.get("jitter_min_sec", 45)),
            step=1,
        )
    with j2:
        jitter_max_sec = st.number_input(
            "非等比随机间隔最大值（秒）",
            min_value=0,
            max_value=7200,
            value=int(selected_pkg.get("jitter_max_sec", 180)),
            step=1,
        )

    active_time_range = st.slider(
        "作息规律模拟（活跃时间段）",
        min_value=datetime.strptime("00:00", "%H:%M").time(),
        max_value=datetime.strptime("23:59", "%H:%M").time(),
        value=default_time_range,
        format="HH:mm",
    )

    s1, s2 = st.columns([1, 1])
    if s1.button("保存当前策略包", type="primary"):
        start_t, end_t = active_time_range
        for i, pkg in enumerate(strategy_packages):
            if str(pkg.get("id")) == str(selected_pkg_id):
                strategy_packages[i] = {
                    **pkg,
                    "name": pkg_name.strip() or pkg.get("name", "未命名策略包"),
                    "description": pkg_desc.strip(),
                    "daily_limit": int(daily_limit),
                    "hourly_limit": int(hourly_limit),
                    "jitter_min_sec": int(min(jitter_min_sec, jitter_max_sec)),
                    "jitter_max_sec": int(max(jitter_min_sec, jitter_max_sec)),
                    "active_start": start_t.strftime("%H:%M"),
                    "active_end": end_t.strftime("%H:%M"),
                    "updated_at": _now_str(),
                }
                break
        save_comment_strategy_packages_for_user(st.session_state.username, strategy_packages)
        st.success("策略包已保存。")
        st.rerun()
    if s2.button("删除当前策略包", type="secondary", disabled=len(strategy_packages) <= 1):
        strategy_packages = [
            p for p in strategy_packages if str(p.get("id")) != str(selected_pkg_id)
        ]
        save_comment_strategy_packages_for_user(st.session_state.username, strategy_packages)
        st.session_state.comment_strategy_package_selected_id = str(strategy_packages[0].get("id"))
        st.success("策略包已删除。")
        st.rerun()
    if len(strategy_packages) <= 1:
        st.caption("至少保留 1 个策略包。")

    st.markdown("---")
    st.subheader("策略执行判定预览")
    st.caption("每次评论都会先做累积判断：超配额则延后，非活跃时段则自动顺延到下一个可执行时间。")

    preview = compute_comment_schedule(
        st.session_state.username,
        now_dt=datetime.now(),
        strategy_override=_strategy_from_package(selected_pkg),
        strategy_package_id=selected_pkg_id,
    )
    next_time = preview["scheduled_dt"].strftime("%Y-%m-%d %H:%M:%S")
    p1, p2, p3 = st.columns(3)
    p1.metric("即时判定", "延后执行" if preview["delayed"] else "可立即执行")
    p2.metric("下一次可执行时间", next_time)
    p3.metric("本次随机间隔", f"{int(preview['jitter_sec'])} 秒")
    if preview["delayed"]:
        st.warning(preview["reason"])
    else:
        st.success(preview["reason"])

    events = preview["events"]
    now_dt = datetime.now()
    today_count = 0
    hour_count = 0
    for e in events:
        dt_value = _to_dt(e.get("scheduled_for"))
        if not dt_value:
            continue
        if dt_value.date() == now_dt.date():
            today_count += 1
        if (
            dt_value.year == now_dt.year
            and dt_value.month == now_dt.month
            and dt_value.day == now_dt.day
            and dt_value.hour == now_dt.hour
        ):
            hour_count += 1
    st.caption(
        f"策略包[{selected_pkg.get('name', '-')}]当前累积：今日已排程 {today_count} 条；当前小时已排程 {hour_count} 条。"
    )

    if events:
        sorted_events = sorted(
            events,
            key=lambda x: str(x.get("scheduled_for", "")),
            reverse=True,
        )[:20]
        records = []
        for ev in sorted_events:
            records.append(
                {
                    "计划执行时间": ev.get("scheduled_for", "-"),
                    "笔记ID": ev.get("note_id", "-"),
                    "目标对象": format_comment_event_target(ev),
                    "话术模板": ev.get("template_name", "-"),
                    "评论账号": ev.get("comment_account_name", "-"),
                    "实际发送账号": ev.get("mcp_username", "-"),
                    "策略包": ev.get("strategy_package_name", "-"),
                    "随机间隔(秒)": ev.get("jitter_sec", "-"),
                    "状态": format_comment_event_status(ev.get("status", "-")),
                    "结果说明": ev.get("error_message") or ev.get("result_message") or "-",
                }
            )
        st.dataframe(pd.DataFrame(records), use_container_width=True, hide_index=True)
        if st.button("清空当前策略包排程记录", type="secondary"):
            all_events = load_comment_strategy_events(st.session_state.username)
            kept_events = [
                e for e in all_events
                if str(e.get("strategy_package_id", "")) != str(selected_pkg_id)
            ]
            save_comment_strategy_events_for_user(st.session_state.username, kept_events)
            conn = get_tasks_connection()
            try:
                job_rows = conn.execute(
                    """
                    SELECT id
                    FROM comment_jobs
                    WHERE dashboard_user = ? AND strategy_package_id = ?
                    """,
                    (str(st.session_state.username), str(selected_pkg_id)),
                ).fetchall()
                job_ids = [int(row[0]) for row in job_rows]
                if job_ids:
                    placeholders = ",".join(["?"] * len(job_ids))
                    conn.execute(
                        f"DELETE FROM comment_verification_runs WHERE comment_job_id IN ({placeholders})",
                        job_ids,
                    )
                    conn.execute(
                        """
                        DELETE FROM comment_jobs
                        WHERE dashboard_user = ? AND strategy_package_id = ?
                        """,
                        (str(st.session_state.username), str(selected_pkg_id)),
                    )
                conn.commit()
            finally:
                conn.close()
            st.success("已清空当前策略包的排程记录。")
            st.rerun()
    else:
        st.info("暂无评论执行记录。点击“去评论”或“回复”后，这里会展示整帖评论与具体评论回复的执行流水。")

if current_page == "执行中心":
    st.header("执行中心")
    st.caption("默认先看结果：哪些评论待执行、哪些已经确认可见、哪个发送账号效果更好，以及当前有哪些阻塞问题。")

    view_col, help_col = st.columns([2.3, 4.7])
    with view_col:
        st.radio(
            "视图切换",
            ["简洁视图", "高级视图"],
            index=0,
            horizontal=True,
            key="comment_execution_view_mode",
        )
    with help_col:
        st.caption("简洁视图默认隐藏任务 ID、账号 ID 等技术字段；高级视图保留完整排障、批量处理和登录管理能力。")

    queue_tab, verify_tab, metrics_tab, exceptions_tab, verifier_tab = st.tabs(
        ["执行队列", "回溯校验", "账号表现", "异常中心", "账号与登录"]
    )

    with queue_tab:
        render_comment_execution_queue_fragment(st.session_state.username)

    with verify_tab:
        render_comment_execution_verification_fragment(st.session_state.username)

    with metrics_tab:
        render_comment_execution_metrics_fragment(st.session_state.username)

    with exceptions_tab:
        render_operation_exceptions_fragment(st.session_state.username)

    with verifier_tab:
        execution_center_advanced_view = _is_comment_execution_advanced_view()
        st.caption("账号登录与管理默认收起，避免打扰结果阅读；需要操作时再展开即可。")

        with st.expander("回溯检查账号", expanded=False):
            st.caption("用于从旁观视角确认评论是否真的外部可见。")

            verifier_accounts = load_verifier_accounts(st.session_state.username)
            if verifier_accounts:
                verifier_table_rows = []
                advanced_verifier_rows = []
                for item in verifier_accounts:
                    verifier_table_rows.append(
                        {
                            "账号备注": _friendly_verifier_account_name(
                                item.get("account_name"),
                                item.get("account_id"),
                            ),
                            "状态": "启用" if str(item.get("status") or "") == "active" else "停用",
                            "默认账号": "是" if int(item.get("is_default", 0) or 0) == 1 else "否",
                            "更新时间": item.get("updated_at") or item.get("created_at") or "-",
                        }
                    )
                    advanced_verifier_rows.append(
                        {
                            "账号备注": _friendly_verifier_account_name(
                                item.get("account_name"),
                                item.get("account_id"),
                            ),
                            "账号ID": item.get("account_id") or "-",
                            "状态": "启用" if str(item.get("status") or "") == "active" else "停用",
                            "默认账号": "是" if int(item.get("is_default", 0) or 0) == 1 else "否",
                            "备注说明": item.get("notes") or "-",
                            "更新时间": item.get("updated_at") or item.get("created_at") or "-",
                        }
                    )
                st.dataframe(pd.DataFrame(verifier_table_rows), use_container_width=True, hide_index=True)
                if execution_center_advanced_view:
                    st.caption("高级视图已补充账号 ID 和备注说明。")
                    st.dataframe(pd.DataFrame(advanced_verifier_rows), use_container_width=True, hide_index=True)

                verifier_account_options = [str(item.get("account_id")) for item in verifier_accounts]
                selected_manage_verifier_id = st.selectbox(
                    "选择要管理的回溯检查账号",
                    options=verifier_account_options,
                    format_func=lambda x: next(
                        (
                            _friendly_verifier_account_name(
                                item.get("account_name"),
                                item.get("account_id"),
                                include_id=execution_center_advanced_view,
                            )
                            for item in verifier_accounts if str(item.get("account_id")) == str(x)
                        ),
                        str(x),
                    ),
                    key="execution_center_verifier_manage_id",
                )
                selected_manage_verifier = next(
                    (item for item in verifier_accounts if str(item.get("account_id")) == str(selected_manage_verifier_id)),
                    verifier_accounts[0],
                )
                vbtn1, vbtn2, vbtn3 = st.columns([1, 1, 1])
                if vbtn1.button("设为默认账号", key="execution_center_verifier_set_default", use_container_width=True):
                    try:
                        set_default_verifier_account(st.session_state.username, selected_manage_verifier_id)
                        st.success("默认回溯检查账号已更新。")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"设置默认账号失败：{exc}")
                toggle_label = "停用该账号" if str(selected_manage_verifier.get("status") or "") == "active" else "启用该账号"
                if vbtn2.button(toggle_label, key="execution_center_verifier_toggle_status", use_container_width=True):
                    try:
                        upsert_verifier_account(
                            st.session_state.username,
                            account_id=selected_manage_verifier.get("account_id"),
                            account_name=selected_manage_verifier.get("account_name"),
                            notes=selected_manage_verifier.get("notes"),
                            status="inactive" if str(selected_manage_verifier.get("status") or "") == "active" else "active",
                            make_default=bool(int(selected_manage_verifier.get("is_default", 0) or 0)),
                        )
                        st.success("回溯检查账号状态已更新。")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"更新账号状态失败：{exc}")
                if vbtn3.button("删除该账号", key="execution_center_verifier_delete", use_container_width=True):
                    try:
                        delete_verifier_account(st.session_state.username, selected_manage_verifier_id)
                        st.success("回溯检查账号已删除。")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"删除账号失败：{exc}")

                st.markdown("#### 回溯检查账号登录")
                st.caption("登录后，系统可以从旁观视角检查评论是否真的已经外部可见。")
                render_mcp_runtime_account_controls(
                    selected_manage_verifier.get("account_id"),
                    _friendly_verifier_account_name(
                        selected_manage_verifier.get("account_name"),
                        selected_manage_verifier.get("account_id"),
                    ),
                    key_prefix="verifier_runtime_controls",
                    show_runtime_id=execution_center_advanced_view,
                    current_account_prefix="当前回溯检查账号",
                    check_button_label="检查登录状态",
                    qrcode_button_label="获取登录二维码",
                    clear_button_label="清除登录状态",
                    status_prefix="当前登录状态",
                    profile_prefix="当前主页账号",
                    show_profile_red_id=execution_center_advanced_view,
                    login_success_message="登录状态已刷新。",
                    already_logged_in_message="该账号当前已登录，无需重复扫码。",
                    qrcode_success_message="二维码已刷新，请使用对应小红书账号扫码登录。",
                    clear_success_message="该账号的登录状态已清除。",
                )
            else:
                st.info("暂无回溯检查账号，请先新增后再做检查。")

            st.markdown("---")
            st.markdown("#### 新增回溯检查账号")
            with st.form("add_verifier_account_form", clear_on_submit=True):
                v1, v2 = st.columns([2, 2])
                with v1:
                    new_verifier_account_id = st.text_input("回溯检查账号ID *", placeholder="例如：xhs_verifier_a")
                    new_verifier_account_name = st.text_input("账号备注", placeholder="例如：验号A")
                with v2:
                    new_verifier_account_notes = st.text_area("补充说明", placeholder="例如：只用于回溯检查，不参与发送", height=80)
                    new_verifier_make_default = st.checkbox("设为默认账号", value=False)
                submit_verifier_account = st.form_submit_button("新增回溯检查账号", type="primary")
                if submit_verifier_account:
                    try:
                        upsert_verifier_account(
                            st.session_state.username,
                            account_id=new_verifier_account_id,
                            account_name=new_verifier_account_name,
                            notes=new_verifier_account_notes,
                            status="active",
                            make_default=new_verifier_make_default,
                        )
                        st.success("回溯检查账号已保存。")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"保存回溯检查账号失败：{exc}")

        with st.expander("发送账号登录", expanded=False):
            st.caption("用于确保发送账号可以直接执行评论发送。")
            sender_accounts = load_comment_executor_accounts(st.session_state.username)
            if not sender_accounts:
                sender_accounts = [{
                    "account_id": MCP_DEFAULT_RUNTIME_ACCOUNT_ID,
                    "account_name": "系统默认发送账号",
                }]
            else:
                sender_accounts = [
                    {
                        "account_id": MCP_DEFAULT_RUNTIME_ACCOUNT_ID,
                        "account_name": "系统默认发送账号",
                    },
                    *sender_accounts,
                ]

            sender_account_ids = [str(item.get("account_id")) for item in sender_accounts]
            selected_sender_runtime_id = st.selectbox(
                "选择发送账号",
                options=sender_account_ids,
                format_func=lambda x: next(
                    (
                        _friendly_comment_executor_account_name(
                            item.get("account_name"),
                            item.get("account_id"),
                            include_id=execution_center_advanced_view,
                        )
                        for item in sender_accounts if str(item.get("account_id")) == str(x)
                    ),
                    str(x),
                ),
                key="execution_center_sender_runtime_id",
            )
            selected_sender_runtime = next(
                (item for item in sender_accounts if str(item.get("account_id")) == str(selected_sender_runtime_id)),
                sender_accounts[0],
            )
            render_mcp_runtime_account_controls(
                selected_sender_runtime.get("account_id"),
                _friendly_comment_executor_account_name(
                    selected_sender_runtime.get("account_name"),
                    selected_sender_runtime.get("account_id"),
                ),
                key_prefix="sender_runtime_controls",
                show_runtime_id=execution_center_advanced_view,
                current_account_prefix="当前发送账号",
                check_button_label="检查登录状态",
                qrcode_button_label="获取登录二维码",
                clear_button_label="清除登录状态",
                status_prefix="当前登录状态",
                profile_prefix="当前主页账号",
                show_profile_red_id=execution_center_advanced_view,
                login_success_message="登录状态已刷新。",
                already_logged_in_message="该账号当前已登录，无需重复扫码。",
                qrcode_success_message="二维码已刷新，请使用对应小红书账号扫码登录。",
                clear_success_message="该账号的登录状态已清除。",
            )

if current_page == "爬取账号":
    st.header("账号管理")
    
    # Stats Section
    st.subheader("📊 爬取统计")
    total_df, daily_df = get_account_stats()
    
    if not total_df.empty:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.write("累计爬取量")
            st.dataframe(total_df, use_container_width=True, hide_index=True)
        with col2:
            st.write("每日爬取趋势 (近7天)")
            # Pivot for better chart format: index=date, columns=account, values=count
            if not daily_df.empty:
                chart_data = daily_df.pivot(index='crawl_date', columns='account_name', values='daily_count').fillna(0)
                st.line_chart(chart_data)
    else:
        st.info("暂无爬取统计数据")
        
    st.markdown("---")
    st.info("在此添加小红书账号Cookie，创建任务时可选择使用指定账号爬取。支持直接粘贴 DevTools Cookie 表格内容，系统会自动转换。")
    
    with st.form("add_account_form"):
        new_acc_name = st.text_input("账号备注名 (例如: 号码A)")
        new_acc_cookie = st.text_area("Cookie字符串（支持整行Cookie或DevTools表格多行粘贴）")
        submitted_acc = st.form_submit_button("添加账号")
        
        if submitted_acc:
            normalized_cookie = normalize_cookie_input(new_acc_cookie)
            if new_acc_name and normalized_cookie:
                accounts = load_accounts()
                # Check duplicate
                if any(acc['name'] == new_acc_name for acc in accounts):
                    st.error("账号名已存在！")
                else:
                    accounts.append({"name": new_acc_name, "cookie": normalized_cookie})
                    save_accounts(accounts)
                    cookie_count = len([p for p in normalized_cookie.split("; ") if "=" in p])
                    st.success(f"账号添加成功！已识别 {cookie_count} 个 Cookie 字段。")
                    
                    # Validation feedback
                    parsed_keys = [p.split("=")[0] for p in normalized_cookie.split("; ")]
                    critical_keys = ["web_session", "a1"]
                    missing_keys = [k for k in critical_keys if k not in parsed_keys]
                    
                    if missing_keys:
                        st.warning(f"⚠️ 警告：未检测到关键 Cookie 字段: {', '.join(missing_keys)}。这可能导致爬取失败。")
                    else:
                        st.info("✅ 关键 Cookie 字段 (web_session, a1) 均已检测到。")
                        
                    with st.expander("查看解析后的 Cookie 键名预览"):
                        st.write(parsed_keys)
                        
                    # Allow user to see the message before rerun
                    time.sleep(2)
                    st.rerun()
            else:
                st.error("请填写完整信息，并确认Cookie可被识别。")
    
    st.markdown("---")
    st.subheader("已保存账号")
    accounts = load_accounts()
    if accounts:
        for i, acc in enumerate(accounts):
            with st.container():
                c1, c2, c3 = st.columns([2, 6, 2])
                c1.write(f"**{acc['name']}**")
                c2.text(f"{acc['cookie'][:50]}..." if len(acc['cookie']) > 50 else acc['cookie'])
                if c3.button("删除", key=f"del_acc_{i}"):
                    accounts.pop(i)
                    save_accounts(accounts)
                    st.rerun()
                st.divider()
    else:
        st.info("暂无保存的账号")

if current_page == "评论账号":
    st.header("小红书评论账号管理")
    st.caption("维护用于调用第三方评论接口的账号池。评论弹窗会按账号ID发起评论请求，你可以在本页直接完成该账号的扫码登录与登录态检查。")

    with st.form("add_comment_executor_account_form", clear_on_submit=True):
        c1, c2 = st.columns([2, 2])
        with c1:
            new_comment_account_id = st.text_input("评论账号ID *", placeholder="例如：xhs_acc_10001")
        with c2:
            new_comment_account_name = st.text_input("账号备注", placeholder="例如：高权重主号A")
        submitted_new_comment_account = st.form_submit_button("新增评论账号", type="primary")

        if submitted_new_comment_account:
            account_id = (new_comment_account_id or "").strip()
            account_name = (new_comment_account_name or "").strip() or account_id
            if not account_id:
                st.error("请填写评论账号ID。")
            else:
                comment_accounts = load_comment_executor_accounts(st.session_state.username)
                exists = any(
                    str(a.get("account_id", "")).strip() == account_id
                    for a in comment_accounts
                )
                if exists:
                    st.error("该评论账号ID已存在。")
                else:
                    now = _now_str()
                    comment_accounts.insert(
                        0,
                        {
                            "account_id": account_id,
                            "account_name": account_name,
                            "status": "active",
                            "created_at": now,
                            "updated_at": now,
                        },
                    )
                    save_comment_executor_accounts_for_user(st.session_state.username, comment_accounts)
                    st.success("评论账号新增成功。")
                    st.rerun()

    with st.expander("批量新增（每行一个账号：账号ID,备注；备注可省略）", expanded=False):
        bulk_text = st.text_area(
            "批量导入内容",
            height=140,
            placeholder="xhs_acc_10001,主号A\nxhs_acc_10002,主号B\nxhs_acc_10003",
        )
        if st.button("批量导入评论账号"):
            raw_lines = [line.strip() for line in (bulk_text or "").splitlines()]
            raw_lines = [line for line in raw_lines if line]
            if not raw_lines:
                st.error("请先输入批量账号内容。")
            else:
                comment_accounts = load_comment_executor_accounts(st.session_state.username)
                existing_ids = {
                    str(a.get("account_id", "")).strip()
                    for a in comment_accounts
                }
                inserted_count = 0
                skip_count = 0
                now = _now_str()
                for line in raw_lines:
                    parts = [p.strip() for p in re.split(r"[,，\t]", line) if p.strip()]
                    if not parts:
                        continue
                    account_id = parts[0]
                    account_name = parts[1] if len(parts) > 1 else account_id
                    if not account_id or account_id in existing_ids:
                        skip_count += 1
                        continue
                    comment_accounts.append(
                        {
                            "account_id": account_id,
                            "account_name": account_name,
                            "status": "active",
                            "created_at": now,
                            "updated_at": now,
                        }
                    )
                    existing_ids.add(account_id)
                    inserted_count += 1

                if inserted_count > 0:
                    save_comment_executor_accounts_for_user(st.session_state.username, comment_accounts)
                    st.success(f"批量导入完成：新增 {inserted_count} 个，跳过 {skip_count} 个。")
                    st.rerun()
                else:
                    st.warning("没有可新增的账号（可能全部重复或格式不正确）。")

    st.markdown("---")
    comment_accounts = load_comment_executor_accounts(st.session_state.username)
    st.metric("评论账号总数", len(comment_accounts))

    if not comment_accounts:
        st.info("暂无评论账号，请先新增。")
    else:
        rows = []
        for item in comment_accounts:
            rows.append(
                {
                    "账号备注": item.get("account_name", "-"),
                    "账号ID": item.get("account_id", "-"),
                    "状态": "启用" if str(item.get("status", "active")) == "active" else "停用",
                    "创建时间": item.get("created_at", "-"),
                    "更新时间": item.get("updated_at", "-"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.markdown("##### 账号登录管理（本页直接登录）")
        runtime_account_options = [str(item.get("account_id", "")).strip() for item in comment_accounts if str(item.get("account_id", "")).strip()]
        selected_runtime_account_id = st.selectbox(
            "选择要登录/检查的评论账号",
            options=runtime_account_options,
            format_func=lambda account_id: next(
                (
                    f"{a.get('account_name', '-')}" f"（ID: {a.get('account_id', '-')}）"
                    for a in comment_accounts if str(a.get("account_id", "")).strip() == str(account_id)
                ),
                str(account_id),
            ),
            key="comment_account_runtime_selected_id",
        )
        selected_runtime_account = next(
            (
                item for item in comment_accounts
                if str(item.get("account_id", "")).strip() == str(selected_runtime_account_id)
            ),
            None,
        )
        if selected_runtime_account:
            render_mcp_runtime_account_controls(
                selected_runtime_account.get("account_id"),
                selected_runtime_account.get("account_name") or selected_runtime_account.get("account_id"),
                key_prefix=f"comment_account_page_runtime_{selected_runtime_account_id}",
            )

        account_id_options = [str(item.get("account_id", "")) for item in comment_accounts]
        delete_targets = st.multiselect(
            "选择要删除的评论账号（可多选）",
            options=account_id_options,
            format_func=lambda x: next(
                (
                    f"{a.get('account_name', '-')}" f"（ID: {a.get('account_id', '-')}）"
                    for a in comment_accounts if str(a.get("account_id", "")) == str(x)
                ),
                str(x),
            ),
        )
        if st.button("删除选中账号", type="secondary", disabled=not delete_targets):
            remained = [
                a for a in comment_accounts
                if str(a.get("account_id", "")) not in {str(t) for t in delete_targets}
            ]
            save_comment_executor_accounts_for_user(st.session_state.username, remained)
            st.success(f"已删除 {len(delete_targets)} 个评论账号。")
            st.rerun()

if current_page == "员工管理":
    st.header("员工管理")
    st.caption("配置电子员工的目标、工作时间、资源绑定和 Prompt 组合，为后续自动化运营 Worker 提供标准化输入。")

    employee_prompts = load_employee_prompts(st.session_state.username)
    employee_profiles = load_employee_profiles(st.session_state.username)
    crawl_accounts = load_accounts()
    comment_accounts = load_comment_executor_accounts(st.session_state.username)
    strategy_packages = load_comment_strategy_packages(st.session_state.username)

    crawl_account_options = ["默认 (扫码)"] + [
        str(acc.get("name", "")).strip()
        for acc in crawl_accounts
        if str(acc.get("name", "")).strip()
    ]
    comment_account_ids = [
        str(acc.get("account_id", "")).strip()
        for acc in comment_accounts
        if str(acc.get("account_id", "")).strip()
    ]
    strategy_package_ids = [
        str(pkg.get("id", "")).strip()
        for pkg in strategy_packages
        if str(pkg.get("id", "")).strip()
    ]

    employee_ids = [str(item.get("id", "")) for item in employee_profiles]
    if st.session_state.employee_profile_selected_id not in employee_ids:
        st.session_state.employee_profile_selected_id = employee_ids[0] if employee_ids else None

    prompt_ids = [str(item.get("id", "")) for item in employee_prompts]
    if st.session_state.employee_prompt_selected_id not in prompt_ids:
        st.session_state.employee_prompt_selected_id = prompt_ids[0] if prompt_ids else None

    st.subheader("1) 新增员工")
    st.caption("资源未绑定也允许保存，但后续自动化执行时可能无法顺利完成目标。")
    create_type_selector_col = st.container()
    with create_type_selector_col:
        st.markdown("##### 员工类型")
        new_employee_type = st.radio(
            "员工类型",
            options=EMPLOYEE_TYPE_OPTIONS,
            horizontal=True,
            format_func=_employee_type_label,
            key="create_employee_type_selector",
            label_visibility="collapsed",
        )

    with st.form("create_employee_profile_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            new_employee_name = st.text_input(
                "员工名称 *",
                placeholder="例如：情感引流小助手",
                key="create_employee_name",
            )
            new_employee_status = st.selectbox(
                "工作状态",
                options=["active", "paused"],
                format_func=_employee_status_label,
                key="create_employee_status",
            )
            new_goal_description = ""
            new_notes = ""
            if new_employee_type != "strategy_lead":
                new_goal_description = st.text_area(
                    "目标描述 *",
                    height=130,
                    placeholder="例如：寻找不知道怎么谈恋爱的大学生，通过评论种草我的课程或建立进一步沟通意愿。",
                    key="create_employee_goal_description",
                )
                new_notes = st.text_area(
                    "补充说明",
                    height=100,
                    placeholder="例如：尽量优先切入焦虑、求助、吐槽类帖子。",
                    key="create_employee_notes",
                )
            worker_employee_options = [
                str(item.get("id", ""))
                for item in employee_profiles
                if str(item.get("employee_type", "")) in {"seeding", "lead_generation"}
            ]
            new_analysis_scope_mode = "all"
            new_analysis_employee_ids = []
            new_analysis_frequency = "daily"
            if new_employee_type == "strategy_lead":
                st.markdown("##### 分析配置")
                new_analysis_scope_mode = st.selectbox(
                    "分析范围",
                    options=STRATEGY_LEAD_SCOPE_OPTIONS,
                    index=0,
                    format_func=lambda value: "全员分析" if value == "all" else "指定员工",
                    key="create_strategy_lead_scope_mode",
                )
                new_analysis_employee_ids = st.multiselect(
                    "指定分析员工",
                    options=worker_employee_options,
                    default=[],
                    disabled=new_analysis_scope_mode != "selected",
                    format_func=lambda employee_id: next(
                        (
                            f"{item.get('name', '-')}" f"（{_employee_type_label(item.get('employee_type', '-'))}）"
                            for item in employee_profiles
                            if str(item.get("id", "")) == str(employee_id)
                        ),
                        str(employee_id),
                    ),
                    key="create_strategy_lead_employee_ids",
                )
                new_analysis_frequency = st.selectbox(
                    "分析频率",
                    options=["daily", "weekly"],
                    index=0,
                    format_func=lambda value: "每日" if value == "daily" else "每周",
                    key="create_strategy_lead_frequency",
                )
        with c2:
            st.markdown("##### 工作时间")
            new_work_time_blocks = _build_employee_work_time_editor(
                "create_employee_work_time",
                _default_employee_work_time_blocks(),
            )
            new_crawl_accounts = []
            new_comment_accounts = []
            new_strategy_package_ids = []
            if new_employee_type == "strategy_lead":
                st.info("分析员工不参与实际爬取和评论执行，无需绑定爬取账号、评论账号或策略包。")
            else:
                new_crawl_accounts = st.multiselect(
                    "绑定爬取账号",
                    options=crawl_account_options,
                    key="create_employee_crawl_accounts",
                )
                new_comment_accounts = st.multiselect(
                    "绑定评论账号",
                    options=comment_account_ids,
                    format_func=lambda account_id: next(
                        (
                            f"{item.get('account_name', '-')}" f"（ID: {item.get('account_id', '-')}）"
                            for item in comment_accounts
                            if str(item.get("account_id", "")) == str(account_id)
                        ),
                        str(account_id),
                    ),
                    key="create_employee_comment_accounts",
                )
                new_strategy_package_ids = st.multiselect(
                    "绑定评论策略包",
                    options=strategy_package_ids,
                    format_func=lambda package_id: next(
                        (
                            f"{item.get('name', '-')}"
                            for item in strategy_packages
                            if str(item.get("id", "")) == str(package_id)
                        ),
                        str(package_id),
                    ),
                    key="create_employee_strategy_packages",
                )

        create_persona_prompts = _get_prompt_candidates(employee_prompts, "persona", new_employee_type, active_only=True)
        create_planning_prompts = _get_prompt_candidates(employee_prompts, "planning", new_employee_type, active_only=True)
        create_kpi_prompts = _get_prompt_candidates(employee_prompts, "kpi", new_employee_type, active_only=True)
        create_schedule_prompts = _get_prompt_candidates(employee_prompts, "schedule", new_employee_type, active_only=True)
        create_persona_prompt_id = _resolve_prompt_id(employee_prompts, "persona", new_employee_type)
        create_planning_prompt_id = _resolve_prompt_id(employee_prompts, "planning", new_employee_type)
        create_kpi_prompt_id = _resolve_prompt_id(employee_prompts, "kpi", new_employee_type)
        create_schedule_prompt_id = _resolve_prompt_id(employee_prompts, "schedule", new_employee_type)
        new_persona_prompt_id = create_persona_prompt_id
        new_planning_prompt_id = create_planning_prompt_id
        new_kpi_prompt_id = create_kpi_prompt_id
        new_schedule_prompt_id = create_schedule_prompt_id

        if new_employee_type != "strategy_lead":
            st.markdown("##### Prompt 绑定")
            p1, p2 = st.columns(2)
            with p1:
                new_persona_prompt_id = st.selectbox(
                    "员工技能 Prompt",
                    options=[str(item.get("id", "")) for item in create_persona_prompts],
                    index=_safe_index([str(item.get("id", "")) for item in create_persona_prompts], create_persona_prompt_id),
                    format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                    key="create_employee_persona_prompt_id",
                )
                new_kpi_prompt_id = st.selectbox(
                    "动态 KPI Prompt",
                    options=[str(item.get("id", "")) for item in create_kpi_prompts],
                    index=_safe_index([str(item.get("id", "")) for item in create_kpi_prompts], create_kpi_prompt_id),
                    format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                    key="create_employee_kpi_prompt_id",
                )
            with p2:
                new_planning_prompt_id = st.selectbox(
                    "目标拆解 Prompt",
                    options=[str(item.get("id", "")) for item in create_planning_prompts],
                    index=_safe_index([str(item.get("id", "")) for item in create_planning_prompts], create_planning_prompt_id),
                    format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                    key="create_employee_planning_prompt_id",
                )
                new_schedule_prompt_id = st.selectbox(
                    "工作排班 Prompt",
                    options=[str(item.get("id", "")) for item in create_schedule_prompts],
                    index=_safe_index([str(item.get("id", "")) for item in create_schedule_prompts], create_schedule_prompt_id),
                    format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                    key="create_employee_schedule_prompt_id",
                )

        create_employee = st.form_submit_button("新增员工", type="primary")
        if create_employee:
            if not new_employee_name.strip():
                st.error("请填写员工名称。")
            elif new_employee_type != "strategy_lead" and not new_goal_description.strip():
                st.error("请填写目标描述。")
            elif not new_work_time_blocks:
                st.error("请至少设置 1 个有效的工作时间段。")
            else:
                new_profile = {
                    "id": f"employee_{int(time.time() * 1000)}",
                    "dashboard_user": st.session_state.username,
                    "name": new_employee_name.strip(),
                    "status": new_employee_status,
                    "employee_type": new_employee_type,
                    "goal_description": new_goal_description.strip() if new_employee_type != "strategy_lead" else "",
                    "work_time_blocks": new_work_time_blocks,
                    "assigned_crawl_accounts": new_crawl_accounts,
                    "assigned_comment_accounts": new_comment_accounts,
                    "assigned_strategy_package_ids": new_strategy_package_ids,
                    "persona_prompt_id": new_persona_prompt_id,
                    "planning_prompt_id": new_planning_prompt_id,
                    "kpi_prompt_id": new_kpi_prompt_id,
                    "schedule_prompt_id": new_schedule_prompt_id,
                    "analysis_scope_mode": new_analysis_scope_mode if new_employee_type == "strategy_lead" else "all",
                    "analysis_employee_ids": new_analysis_employee_ids if new_employee_type == "strategy_lead" else [],
                    "analysis_frequency": new_analysis_frequency if new_employee_type == "strategy_lead" else "daily",
                    "notes": new_notes.strip() if new_employee_type != "strategy_lead" else "",
                    "created_at": _now_str(),
                    "updated_at": _now_str(),
                }
                employee_profiles.insert(0, new_profile)
                save_employee_profiles_for_user(st.session_state.username, employee_profiles)
                st.session_state.employee_profile_selected_id = new_profile["id"]
                st.success("员工创建成功。")
                st.rerun()

    st.markdown("---")
    st.subheader("2) 员工列表")
    if not employee_profiles:
        st.info("暂无电子员工，请先创建。")
    else:
        rows = []
        for item in employee_profiles:
            rows.append(
                {
                    "员工名": item.get("name", "-"),
                    "类型": _employee_type_label(item.get("employee_type", "seeding")),
                    "状态": _employee_status_label(item.get("status", "active")),
                    "目标摘要": _summarize_text(item.get("goal_description", ""), max_chars=24),
                    "工作时间": _format_work_time_blocks(item.get("work_time_blocks", [])),
                    "绑定资源数": (
                        len(item.get("assigned_crawl_accounts", []))
                        + len(item.get("assigned_comment_accounts", []))
                        + len(item.get("assigned_strategy_package_ids", []))
                    ),
                    "最近更新时间": item.get("updated_at", "-"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("3) 编辑员工")
    if not employee_profiles:
        st.caption("创建至少 1 个员工后可在此编辑。")
    else:
        selected_employee_id = st.selectbox(
            "选择要编辑的员工",
            options=employee_ids,
            index=_safe_index(employee_ids, st.session_state.employee_profile_selected_id),
            format_func=lambda employee_id: next(
                (
                    f"{item.get('name', '-')}" f"（{_employee_type_label(item.get('employee_type', 'seeding'))}）"
                    for item in employee_profiles
                    if str(item.get("id", "")) == str(employee_id)
                ),
                str(employee_id),
            ),
        )
        st.session_state.employee_profile_selected_id = selected_employee_id
        selected_employee = next(
            item for item in employee_profiles if str(item.get("id", "")) == str(selected_employee_id)
        )
        selected_type = str(selected_employee.get("employee_type", "seeding"))
        st.markdown("##### 员工类型")
        edit_employee_type = st.radio(
            "员工类型",
            options=EMPLOYEE_TYPE_OPTIONS,
            index=_safe_index(EMPLOYEE_TYPE_OPTIONS, selected_type),
            horizontal=True,
            format_func=_employee_type_label,
            key=f"edit_employee_type_selector_{selected_employee_id}",
            label_visibility="collapsed",
        )

        with st.form(f"edit_employee_profile_form_{selected_employee_id}"):
            e1, e2 = st.columns(2)
            with e1:
                edit_employee_name = st.text_input(
                    "员工名称",
                    value=str(selected_employee.get("name", "")),
                    key=f"edit_employee_name_{selected_employee_id}",
                )
                edit_employee_status = st.selectbox(
                    "工作状态",
                    options=["active", "paused"],
                    index=_safe_index(["active", "paused"], str(selected_employee.get("status", "active"))),
                    format_func=_employee_status_label,
                    key=f"edit_employee_status_{selected_employee_id}",
                )
                edit_goal_description = str(selected_employee.get("goal_description", ""))
                edit_notes = str(selected_employee.get("notes", ""))
                if edit_employee_type != "strategy_lead":
                    edit_goal_description = st.text_area(
                        "目标描述",
                        value=str(selected_employee.get("goal_description", "")),
                        height=130,
                        key=f"edit_employee_goal_{selected_employee_id}",
                    )
                    edit_notes = st.text_area(
                        "补充说明",
                        value=str(selected_employee.get("notes", "")),
                        height=100,
                        key=f"edit_employee_notes_{selected_employee_id}",
                    )
                edit_analysis_scope_mode = (
                    str(selected_employee.get("analysis_scope_mode", "all"))
                    if str(selected_employee.get("analysis_scope_mode", "all")) in set(STRATEGY_LEAD_SCOPE_OPTIONS)
                    else "all"
                )
                edit_analysis_employee_ids = [
                    str(item).strip()
                    for item in selected_employee.get("analysis_employee_ids", [])
                    if str(item).strip()
                ]
                edit_analysis_frequency = "weekly" if str(selected_employee.get("analysis_frequency", "daily")) == "weekly" else "daily"
                editable_worker_options = [
                    str(item.get("id", ""))
                    for item in employee_profiles
                    if str(item.get("id", "")) != str(selected_employee_id)
                    and str(item.get("employee_type", "")) in {"seeding", "lead_generation"}
                ]
                if edit_employee_type == "strategy_lead":
                    st.markdown("##### 分析配置")
                    edit_analysis_scope_mode = st.selectbox(
                        "分析范围",
                        options=STRATEGY_LEAD_SCOPE_OPTIONS,
                        index=_safe_index(STRATEGY_LEAD_SCOPE_OPTIONS, edit_analysis_scope_mode),
                        format_func=lambda value: "全员分析" if value == "all" else "指定员工",
                        key=f"edit_strategy_lead_scope_mode_{selected_employee_id}",
                    )
                    edit_analysis_employee_ids = st.multiselect(
                        "指定分析员工",
                        options=editable_worker_options,
                        default=[item for item in edit_analysis_employee_ids if item in editable_worker_options],
                        disabled=edit_analysis_scope_mode != "selected",
                        format_func=lambda employee_id: next(
                            (
                                f"{item.get('name', '-')}" f"（{_employee_type_label(item.get('employee_type', '-'))}）"
                                for item in employee_profiles
                                if str(item.get("id", "")) == str(employee_id)
                            ),
                            str(employee_id),
                        ),
                        key=f"edit_strategy_lead_employee_ids_{selected_employee_id}",
                    )
                    edit_analysis_frequency = st.selectbox(
                        "分析频率",
                        options=["daily", "weekly"],
                        index=_safe_index(["daily", "weekly"], edit_analysis_frequency),
                        format_func=lambda value: "每日" if value == "daily" else "每周",
                        key=f"edit_strategy_lead_frequency_{selected_employee_id}",
                    )
            with e2:
                st.markdown("##### 工作时间")
                edit_work_time_blocks = _build_employee_work_time_editor(
                    f"edit_employee_work_time_{selected_employee_id}",
                    selected_employee.get("work_time_blocks", []),
                )
                edit_crawl_accounts = []
                edit_comment_accounts = []
                edit_strategy_package_ids = []
                if edit_employee_type == "strategy_lead":
                    st.info("分析员工不参与实际爬取和评论执行，无需绑定爬取账号、评论账号或策略包。")
                else:
                    edit_crawl_accounts = st.multiselect(
                        "绑定爬取账号",
                        options=crawl_account_options,
                        default=[
                            item for item in selected_employee.get("assigned_crawl_accounts", [])
                            if item in crawl_account_options
                        ],
                        key=f"edit_employee_crawl_accounts_{selected_employee_id}",
                    )
                    edit_comment_accounts = st.multiselect(
                        "绑定评论账号",
                        options=comment_account_ids,
                        default=[
                            item for item in selected_employee.get("assigned_comment_accounts", [])
                            if item in comment_account_ids
                        ],
                        format_func=lambda account_id: next(
                            (
                                f"{item.get('account_name', '-')}" f"（ID: {item.get('account_id', '-')}）"
                                for item in comment_accounts
                                if str(item.get("account_id", "")) == str(account_id)
                            ),
                            str(account_id),
                        ),
                        key=f"edit_employee_comment_accounts_{selected_employee_id}",
                    )
                    edit_strategy_package_ids = st.multiselect(
                        "绑定评论策略包",
                        options=strategy_package_ids,
                        default=[
                            item for item in selected_employee.get("assigned_strategy_package_ids", [])
                            if item in strategy_package_ids
                        ],
                        format_func=lambda package_id: next(
                            (
                                f"{item.get('name', '-')}"
                                for item in strategy_packages
                                if str(item.get("id", "")) == str(package_id)
                            ),
                            str(package_id),
                        ),
                        key=f"edit_employee_strategy_packages_{selected_employee_id}",
                    )

            edit_persona_prompts = _get_prompt_candidates(employee_prompts, "persona", edit_employee_type, active_only=True)
            edit_planning_prompts = _get_prompt_candidates(employee_prompts, "planning", edit_employee_type, active_only=True)
            edit_kpi_prompts = _get_prompt_candidates(employee_prompts, "kpi", edit_employee_type, active_only=True)
            edit_schedule_prompts = _get_prompt_candidates(employee_prompts, "schedule", edit_employee_type, active_only=True)
            edit_persona_prompt_id = _resolve_prompt_id(
                employee_prompts,
                "persona",
                edit_employee_type,
                selected_employee.get("persona_prompt_id"),
            )
            edit_planning_prompt_id = _resolve_prompt_id(
                employee_prompts,
                "planning",
                edit_employee_type,
                selected_employee.get("planning_prompt_id"),
            )
            edit_kpi_prompt_id = _resolve_prompt_id(
                employee_prompts,
                "kpi",
                edit_employee_type,
                selected_employee.get("kpi_prompt_id"),
            )
            edit_schedule_prompt_id = _resolve_prompt_id(
                employee_prompts,
                "schedule",
                edit_employee_type,
                selected_employee.get("schedule_prompt_id"),
            )
            bound_persona_prompt_id = edit_persona_prompt_id
            bound_planning_prompt_id = edit_planning_prompt_id
            bound_kpi_prompt_id = edit_kpi_prompt_id
            bound_schedule_prompt_id = edit_schedule_prompt_id
            if edit_employee_type != "strategy_lead":
                st.markdown("##### Prompt 绑定")
                p3, p4 = st.columns(2)
                with p3:
                    bound_persona_prompt_id = st.selectbox(
                        "员工技能 Prompt",
                        options=[str(item.get("id", "")) for item in edit_persona_prompts],
                        index=_safe_index([str(item.get("id", "")) for item in edit_persona_prompts], edit_persona_prompt_id),
                        format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                        key=f"edit_employee_persona_prompt_{selected_employee_id}",
                    )
                    bound_kpi_prompt_id = st.selectbox(
                        "动态 KPI Prompt",
                        options=[str(item.get("id", "")) for item in edit_kpi_prompts],
                        index=_safe_index([str(item.get("id", "")) for item in edit_kpi_prompts], edit_kpi_prompt_id),
                        format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                        key=f"edit_employee_kpi_prompt_{selected_employee_id}",
                    )
                with p4:
                    bound_planning_prompt_id = st.selectbox(
                        "目标拆解 Prompt",
                        options=[str(item.get("id", "")) for item in edit_planning_prompts],
                        index=_safe_index([str(item.get("id", "")) for item in edit_planning_prompts], edit_planning_prompt_id),
                        format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                        key=f"edit_employee_planning_prompt_{selected_employee_id}",
                    )
                    bound_schedule_prompt_id = st.selectbox(
                        "工作排班 Prompt",
                        options=[str(item.get("id", "")) for item in edit_schedule_prompts],
                        index=_safe_index([str(item.get("id", "")) for item in edit_schedule_prompts], edit_schedule_prompt_id),
                        format_func=lambda prompt_id: _prompt_option_label(_get_prompt_by_id(employee_prompts, prompt_id)),
                        key=f"edit_employee_schedule_prompt_{selected_employee_id}",
                    )

            save_employee = st.form_submit_button("保存员工配置", type="primary")
            if save_employee:
                if not edit_employee_name.strip():
                    st.error("请填写员工名称。")
                elif edit_employee_type != "strategy_lead" and not edit_goal_description.strip():
                    st.error("请填写目标描述。")
                elif not edit_work_time_blocks:
                    st.error("请至少设置 1 个有效的工作时间段。")
                else:
                    for idx, item in enumerate(employee_profiles):
                        if str(item.get("id", "")) == str(selected_employee_id):
                            employee_profiles[idx] = {
                                **item,
                                "name": edit_employee_name.strip(),
                                "status": edit_employee_status,
                                "employee_type": edit_employee_type,
                                "goal_description": edit_goal_description.strip() if edit_employee_type != "strategy_lead" else "",
                                "work_time_blocks": edit_work_time_blocks,
                                "assigned_crawl_accounts": edit_crawl_accounts,
                                "assigned_comment_accounts": edit_comment_accounts,
                                "assigned_strategy_package_ids": edit_strategy_package_ids,
                                "persona_prompt_id": bound_persona_prompt_id,
                                "planning_prompt_id": bound_planning_prompt_id,
                                "kpi_prompt_id": bound_kpi_prompt_id,
                                "schedule_prompt_id": bound_schedule_prompt_id,
                                "analysis_scope_mode": edit_analysis_scope_mode if edit_employee_type == "strategy_lead" else "all",
                                "analysis_employee_ids": edit_analysis_employee_ids if edit_employee_type == "strategy_lead" else [],
                                "analysis_frequency": edit_analysis_frequency if edit_employee_type == "strategy_lead" else "daily",
                                "notes": edit_notes.strip() if edit_employee_type != "strategy_lead" else "",
                                "updated_at": _now_str(),
                            }
                            break
                    save_employee_profiles_for_user(st.session_state.username, employee_profiles)
                    st.success("员工配置已保存。")
                    st.rerun()

        delete_disabled = len(employee_profiles) == 0
        if st.button(
            "删除当前员工",
            type="secondary",
            disabled=delete_disabled,
            key=f"delete_employee_profile_{selected_employee_id}",
        ):
            employee_profiles = [
                item for item in employee_profiles
                if str(item.get("id", "")) != str(selected_employee_id)
            ]
            save_employee_profiles_for_user(st.session_state.username, employee_profiles)
            remaining_ids = [str(item.get("id", "")) for item in employee_profiles]
            st.session_state.employee_profile_selected_id = remaining_ids[0] if remaining_ids else None
            st.success("员工已删除。")
            st.rerun()

    st.markdown("---")
    st.subheader("4) Prompt 管理")
    st.caption("默认 Prompt 为只读模板，可复制为自定义版本，再绑定给某个员工使用。")

    with st.form("create_employee_prompt_form", clear_on_submit=True):
        p5, p6 = st.columns(2)
        with p5:
            new_prompt_name = st.text_input(
                "Prompt 名称 *",
                placeholder="例如：情感赛道-高焦虑种草 Prompt",
                key="create_prompt_name",
            )
            new_prompt_type = st.selectbox(
                "Prompt 类型",
                options=["persona", "planning", "kpi", "schedule"],
                format_func=_prompt_type_label,
                key="create_prompt_type",
            )
        with p6:
            new_prompt_scope = st.selectbox(
                "适用范围",
                options=EMPLOYEE_PROMPT_SCOPE_OPTIONS,
                format_func=_prompt_scope_label,
                key="create_prompt_scope",
            )
            new_prompt_status = st.selectbox(
                "状态",
                options=["active", "inactive"],
                format_func=lambda value: "启用" if value == "active" else "停用",
                key="create_prompt_status",
            )
        new_prompt_content = st.text_area(
            "Prompt 正文 *",
            height=220,
            placeholder="输入你希望电子员工在该环节遵守的 Prompt...",
            key="create_prompt_content",
        )
        create_prompt = st.form_submit_button("新增自定义 Prompt", type="primary")
        if create_prompt:
            if not new_prompt_name.strip():
                st.error("请填写 Prompt 名称。")
            elif not new_prompt_content.strip():
                st.error("请填写 Prompt 正文。")
            else:
                prompt_record = {
                    "id": f"prompt_{int(time.time() * 1000)}",
                    "dashboard_user": st.session_state.username,
                    "name": new_prompt_name.strip(),
                    "prompt_type": new_prompt_type,
                    "employee_type_scope": new_prompt_scope,
                    "content": new_prompt_content.strip(),
                    "status": new_prompt_status,
                    "is_default": False,
                    "created_at": _now_str(),
                    "updated_at": _now_str(),
                }
                employee_prompts.insert(0, prompt_record)
                save_employee_prompts_for_user(st.session_state.username, employee_prompts)
                st.session_state.employee_prompt_selected_id = prompt_record["id"]
                st.success("Prompt 创建成功。")
                st.rerun()

    st.markdown("---")
    filter_col1, filter_col2 = st.columns(2)
    prompt_type_filter = filter_col1.selectbox(
        "按类型过滤",
        options=["全部", "persona", "planning", "kpi", "schedule"],
        format_func=lambda value: "全部" if value == "全部" else _prompt_type_label(value),
        key="employee_prompt_type_filter",
    )
    prompt_scope_filter = filter_col2.selectbox(
        "按范围过滤",
        options=["全部", *EMPLOYEE_PROMPT_SCOPE_OPTIONS],
        format_func=lambda value: "全部" if value == "全部" else _prompt_scope_label(value),
        key="employee_prompt_scope_filter",
    )

    filtered_prompts = []
    for item in employee_prompts:
        if prompt_type_filter != "全部" and str(item.get("prompt_type", "")) != str(prompt_type_filter):
            continue
        if prompt_scope_filter != "全部" and str(item.get("employee_type_scope", "")) != str(prompt_scope_filter):
            continue
        filtered_prompts.append(item)

    if not filtered_prompts:
        st.info("当前筛选条件下暂无 Prompt。")
    else:
        prompt_rows = []
        for item in filtered_prompts:
            prompt_rows.append(
                {
                    "名称": item.get("name", "-"),
                    "类型": _prompt_type_label(item.get("prompt_type", "-")),
                    "适用范围": _prompt_scope_label(item.get("employee_type_scope", "all")),
                    "状态": "默认" if item.get("is_default") else ("启用" if str(item.get("status", "active")) == "active" else "停用"),
                    "更新时间": item.get("updated_at", "-"),
                }
            )
        st.dataframe(pd.DataFrame(prompt_rows), use_container_width=True, hide_index=True)

        filtered_prompt_ids = [str(item.get("id", "")) for item in filtered_prompts]
        if st.session_state.employee_prompt_selected_id not in filtered_prompt_ids:
            st.session_state.employee_prompt_selected_id = filtered_prompt_ids[0]

        selected_prompt_id = st.selectbox(
            "选择要查看/编辑的 Prompt",
            options=filtered_prompt_ids,
            index=_safe_index(filtered_prompt_ids, st.session_state.employee_prompt_selected_id),
            format_func=lambda prompt_id: next(
                (
                    f"{item.get('name', '-')}" f"（{_prompt_type_label(item.get('prompt_type', '-'))}）"
                    for item in filtered_prompts
                    if str(item.get("id", "")) == str(prompt_id)
                ),
                str(prompt_id),
            ),
        )
        st.session_state.employee_prompt_selected_id = selected_prompt_id
        selected_prompt = _get_prompt_by_id(employee_prompts, selected_prompt_id)

        if selected_prompt and selected_prompt.get("is_default"):
            st.markdown("##### 默认 Prompt（只读）")
            st.text_input(
                "Prompt 名称",
                value=str(selected_prompt.get("name", "")),
                disabled=True,
                key=f"default_prompt_name_{selected_prompt_id}",
            )
            d1, d2 = st.columns(2)
            d1.text_input(
                "Prompt 类型",
                value=_prompt_type_label(selected_prompt.get("prompt_type", "")),
                disabled=True,
                key=f"default_prompt_type_{selected_prompt_id}",
            )
            d2.text_input(
                "适用范围",
                value=_prompt_scope_label(selected_prompt.get("employee_type_scope", "all")),
                disabled=True,
                key=f"default_prompt_scope_{selected_prompt_id}",
            )
            st.text_area(
                "Prompt 正文",
                value=str(selected_prompt.get("content", "")),
                height=300,
                disabled=True,
                key=f"default_prompt_content_{selected_prompt_id}",
            )
            if st.button("复制为自定义 Prompt", key=f"copy_default_prompt_{selected_prompt_id}"):
                copied_prompt = {
                    "id": f"prompt_{int(time.time() * 1000)}",
                    "dashboard_user": st.session_state.username,
                    "name": f"{selected_prompt.get('name', '默认 Prompt')}-自定义",
                    "prompt_type": selected_prompt.get("prompt_type", "planning"),
                    "employee_type_scope": selected_prompt.get("employee_type_scope", "all"),
                    "content": selected_prompt.get("content", ""),
                    "status": "active",
                    "is_default": False,
                    "created_at": _now_str(),
                    "updated_at": _now_str(),
                }
                employee_prompts.insert(0, copied_prompt)
                save_employee_prompts_for_user(st.session_state.username, employee_prompts)
                st.session_state.employee_prompt_selected_id = copied_prompt["id"]
                st.success("已复制为自定义 Prompt。")
                st.rerun()
        elif selected_prompt:
            with st.form(f"edit_employee_prompt_form_{selected_prompt_id}"):
                ep1, ep2 = st.columns(2)
                with ep1:
                    edit_prompt_name = st.text_input(
                        "Prompt 名称",
                        value=str(selected_prompt.get("name", "")),
                        key=f"edit_prompt_name_{selected_prompt_id}",
                    )
                    edit_prompt_type = st.selectbox(
                        "Prompt 类型",
                        options=["persona", "planning", "kpi", "schedule"],
                        index=_safe_index(
                            ["persona", "planning", "kpi", "schedule"],
                            str(selected_prompt.get("prompt_type", "planning")),
                        ),
                        format_func=_prompt_type_label,
                        key=f"edit_prompt_type_{selected_prompt_id}",
                    )
                with ep2:
                    edit_prompt_scope = st.selectbox(
                        "适用范围",
                        options=EMPLOYEE_PROMPT_SCOPE_OPTIONS,
                        index=_safe_index(
                            EMPLOYEE_PROMPT_SCOPE_OPTIONS,
                            str(selected_prompt.get("employee_type_scope", "all")),
                        ),
                        format_func=_prompt_scope_label,
                        key=f"edit_prompt_scope_{selected_prompt_id}",
                    )
                    edit_prompt_status = st.selectbox(
                        "状态",
                        options=["active", "inactive"],
                        index=_safe_index(
                            ["active", "inactive"],
                            str(selected_prompt.get("status", "active")),
                        ),
                        format_func=lambda value: "启用" if value == "active" else "停用",
                        key=f"edit_prompt_status_{selected_prompt_id}",
                    )
                edit_prompt_content = st.text_area(
                    "Prompt 正文",
                    value=str(selected_prompt.get("content", "")),
                    height=300,
                    key=f"edit_prompt_content_{selected_prompt_id}",
                )
                save_prompt = st.form_submit_button("保存 Prompt", type="primary")
                if save_prompt:
                    if not edit_prompt_name.strip():
                        st.error("请填写 Prompt 名称。")
                    elif not edit_prompt_content.strip():
                        st.error("请填写 Prompt 正文。")
                    else:
                        for idx, item in enumerate(employee_prompts):
                            if str(item.get("id", "")) == str(selected_prompt_id):
                                employee_prompts[idx] = {
                                    **item,
                                    "name": edit_prompt_name.strip(),
                                    "prompt_type": edit_prompt_type,
                                    "employee_type_scope": edit_prompt_scope,
                                    "status": edit_prompt_status,
                                    "content": edit_prompt_content.strip(),
                                    "updated_at": _now_str(),
                                }
                                break
                        save_employee_prompts_for_user(st.session_state.username, employee_prompts)
                        st.success("Prompt 已保存。")
                        st.rerun()

            if st.button("删除当前 Prompt", type="secondary", key=f"delete_prompt_{selected_prompt_id}"):
                employee_prompts = [
                    item for item in employee_prompts
                    if str(item.get("id", "")) != str(selected_prompt_id)
                ]
                save_employee_prompts_for_user(st.session_state.username, employee_prompts)

                updated_profiles = load_employee_profiles(st.session_state.username)
                for profile in updated_profiles:
                    profile["persona_prompt_id"] = _resolve_prompt_id(
                        employee_prompts,
                        "persona",
                        profile.get("employee_type", "seeding"),
                        profile.get("persona_prompt_id"),
                    )
                    profile["planning_prompt_id"] = _resolve_prompt_id(
                        employee_prompts,
                        "planning",
                        profile.get("employee_type", "seeding"),
                        profile.get("planning_prompt_id"),
                    )
                    profile["kpi_prompt_id"] = _resolve_prompt_id(
                        employee_prompts,
                        "kpi",
                        profile.get("employee_type", "seeding"),
                        profile.get("kpi_prompt_id"),
                    )
                    profile["schedule_prompt_id"] = _resolve_prompt_id(
                        employee_prompts,
                        "schedule",
                        profile.get("employee_type", "seeding"),
                        profile.get("schedule_prompt_id"),
                    )
                save_employee_profiles_for_user(st.session_state.username, updated_profiles)

                remaining_prompt_ids = [str(item.get("id", "")) for item in employee_prompts]
                st.session_state.employee_prompt_selected_id = remaining_prompt_ids[0] if remaining_prompt_ids else None
                st.success("Prompt 已删除。")
                st.rerun()

    st.markdown("---")
    st.subheader("5) 当前生效 SOP 预览")
    preview_employee = None
    if employee_profiles:
        preview_employee = next(
            (
                item for item in employee_profiles
                if str(item.get("id", "")) == str(st.session_state.employee_profile_selected_id)
            ),
            employee_profiles[0],
        )

    if not preview_employee:
        st.info("创建并选择一个员工后，这里会展示最终生效的 SOP 预览。")
    else:
        preview_type = str(preview_employee.get("employee_type", "seeding"))
        preview_persona = _get_prompt_by_id(
            employee_prompts,
            _resolve_prompt_id(employee_prompts, "persona", preview_type, preview_employee.get("persona_prompt_id")),
        )
        preview_planning = _get_prompt_by_id(
            employee_prompts,
            _resolve_prompt_id(employee_prompts, "planning", preview_type, preview_employee.get("planning_prompt_id")),
        )
        preview_kpi = _get_prompt_by_id(
            employee_prompts,
            _resolve_prompt_id(employee_prompts, "kpi", preview_type, preview_employee.get("kpi_prompt_id")),
        )
        preview_schedule = _get_prompt_by_id(
            employee_prompts,
            _resolve_prompt_id(employee_prompts, "schedule", preview_type, preview_employee.get("schedule_prompt_id")),
        )
        bound_crawl_accounts = preview_employee.get("assigned_crawl_accounts", [])
        bound_comment_accounts = [
            next(
                (
                    f"{item.get('account_name', '-')}" f"（ID: {item.get('account_id', '-')}）"
                    for item in comment_accounts
                    if str(item.get("account_id", "")) == str(account_id)
                ),
                str(account_id),
            )
            for account_id in preview_employee.get("assigned_comment_accounts", [])
        ]
        bound_strategy_packages = [
            next(
                (
                    item.get("name", "-")
                    for item in strategy_packages
                    if str(item.get("id", "")) == str(package_id)
                ),
                str(package_id),
            )
            for package_id in preview_employee.get("assigned_strategy_package_ids", [])
        ]

        info_cols = st.columns(4)
        info_cols[0].metric("员工类型", _employee_type_label(preview_type))
        info_cols[1].metric("工作状态", _employee_status_label(preview_employee.get("status", "active")))
        info_cols[2].metric("工作时间段", str(len(preview_employee.get("work_time_blocks", []))))
        info_cols[3].metric("绑定策略包", str(len(preview_employee.get("assigned_strategy_package_ids", []))))

        s1, s2 = st.columns(2)
        with s1:
            st.markdown(f"**员工名称**：{preview_employee.get('name', '-')}")
            if preview_type != "strategy_lead":
                st.markdown(f"**目标描述**：{preview_employee.get('goal_description', '-')}")
            st.markdown(f"**工作时间**：{_format_work_time_blocks(preview_employee.get('work_time_blocks', []))}")
            st.markdown(f"**爬取账号**：{', '.join(bound_crawl_accounts) if bound_crawl_accounts else '未绑定'}")
        with s2:
            st.markdown(f"**评论账号**：{', '.join(bound_comment_accounts) if bound_comment_accounts else '未绑定'}")
            st.markdown(f"**评论策略包**：{', '.join(bound_strategy_packages) if bound_strategy_packages else '未绑定'}")
            if preview_type != "strategy_lead":
                st.markdown(f"**补充说明**：{preview_employee.get('notes', '-') or '-'}")

        if preview_type != "strategy_lead":
            prompt_tabs = st.tabs(
                [
                    "员工技能 Prompt",
                    "目标拆解 Prompt",
                    "动态 KPI Prompt",
                    "工作排班 Prompt",
                ]
            )
            preview_items = [
                preview_persona,
                preview_planning,
                preview_kpi,
                preview_schedule,
            ]
            for tab, prompt in zip(prompt_tabs, preview_items):
                with tab:
                    if not prompt:
                        st.warning("当前未绑定可用 Prompt。")
                    else:
                        st.caption(
                            f"当前绑定：{prompt.get('name', '-')}"
                            f" | {_prompt_scope_label(prompt.get('employee_type_scope', 'all'))}"
                        )
                        st.code(str(prompt.get("content", "")).strip(), language="markdown")

if current_page == "老板看板":
    st.header("老板看板")
    st.caption("查看员工的动态 KPI 判断、老板日报和今日关键操作流水账。页面优先读取缓存快照，只有在你主动生成或重生成时才调用 LLM。")

    llm_settings = _get_agent_report_settings()
    controls = st.columns([1.1, 1.2, 1.4, 1.4, 1.6])
    with controls[0]:
        st.checkbox("只看今天", key="boss_dashboard_only_today")
    with controls[1]:
        st.date_input(
            "查看日期",
            key="boss_dashboard_report_date",
            disabled=st.session_state.boss_dashboard_only_today,
        )

    selected_report_date = (
        datetime.now().date()
        if st.session_state.boss_dashboard_only_today
        else st.session_state.boss_dashboard_report_date
    )
    report_date_text = _report_date_to_str(selected_report_date)

    employee_profiles = load_employee_profiles(st.session_state.username)
    existing_snapshots = get_employee_kpi_snapshots(st.session_state.username, report_date_text)
    existing_report = get_boss_daily_report(st.session_state.username, report_date_text)

    with controls[2]:
        generate_kpi_clicked = st.button(
            "生成当日员工KPI判断",
            type="primary",
            use_container_width=True,
        )
    with controls[3]:
        regenerate_kpi_clicked = st.button(
            "重新生成员工KPI判断",
            use_container_width=True,
        )
    with controls[4]:
        report_button_label = "重新生成老板日报" if existing_report else "生成今日日报"
        generate_report_clicked = st.button(
            report_button_label,
            use_container_width=True,
        )

    if not llm_settings["api_key"]:
        st.warning("当前未配置 AGENT_REPORT_API_KEY 或 LIST_FILTER_API_KEY。页面可以看已有快照，但无法生成新的 KPI 判断和老板日报。")
    else:
        st.caption(f"当前报告模型：{llm_settings['model']} | 接口：{llm_settings['base_url']}")

    generation_messages = []
    generation_errors = []

    if generate_kpi_clicked or regenerate_kpi_clicked:
        if not employee_profiles:
            st.warning("当前没有电子员工，无法生成动态 KPI 判断。")
        else:
            with st.spinner("正在生成员工动态 KPI 判断..."):
                result = generate_employee_kpi_snapshots_for_report_date(
                    st.session_state.username,
                    report_date_text,
                    force=regenerate_kpi_clicked,
                )
            generation_messages.append(f"已处理 {len(result['snapshots'])} 个员工的 KPI 判断。")
            generation_errors.extend(result["errors"])

    if generate_report_clicked:
        if not employee_profiles:
            st.warning("当前没有电子员工，无法生成老板日报。")
        else:
            with st.spinner("正在准备老板日报上下文..."):
                snapshots_for_report = get_employee_kpi_snapshots(st.session_state.username, report_date_text)
                if not snapshots_for_report:
                    result = generate_employee_kpi_snapshots_for_report_date(
                        st.session_state.username,
                        report_date_text,
                        force=False,
                    )
                    generation_messages.append("由于当天还没有 KPI 快照，系统已先自动生成员工动态 KPI 判断。")
                    generation_errors.extend(result["errors"])
                    snapshots_for_report = get_employee_kpi_snapshots(st.session_state.username, report_date_text)
                if snapshots_for_report:
                    generate_boss_daily_report(
                        st.session_state.username,
                        report_date_text,
                        force=bool(existing_report),
                        snapshots=snapshots_for_report,
                    )
                    generation_messages.append("老板日报已生成。")
                else:
                    generation_errors.append("没有可用的员工 KPI 快照，老板日报未生成。")

    if generation_messages or generation_errors:
        if generation_messages:
            st.success(" ".join(generation_messages))
        if generation_errors:
            st.warning("；".join(generation_errors))

    employee_profiles = load_employee_profiles(st.session_state.username)
    snapshots = get_employee_kpi_snapshots(st.session_state.username, report_date_text)
    boss_report = get_boss_daily_report(st.session_state.username, report_date_text)
    summary = get_boss_dashboard_summary(st.session_state.username, report_date_text)
    report_context = build_boss_report_context(st.session_state.username, report_date_text, snapshots=snapshots)
    strategy_lead_reports = build_strategy_lead_reports(st.session_state.username, report_date_text)
    timeline_rows = build_boss_timeline(
        st.session_state.username,
        report_date_text,
        snapshots=snapshots,
        boss_report=boss_report,
    )

    st.markdown("---")
    st.subheader("1) 老板总览")
    summary_cols = st.columns(6)
    summary_cols[0].metric("员工数", f"{summary['active_employee_count']} / {summary['employee_count']}")
    summary_cols[1].metric("今日新增笔记", summary["note_count"])
    summary_cols[2].metric("今日已发送评论", summary["sent_comment_count"])
    summary_cols[3].metric("今日失败评论", summary["failed_comment_count"])
    summary_cols[4].metric("总目标抓取", summary["total_target_crawl_count"])
    summary_cols[5].metric("总目标评论", summary["total_target_comment_count"])

    summary_cols_2 = st.columns(6)
    summary_cols_2[0].metric("今日任务数", summary["task_count"])
    summary_cols_2[1].metric("今日总互动量", summary["interaction_total"])
    summary_cols_2[2].metric("累计 Token", _format_token_usage(summary["total_tokens"]))
    summary_cols_2[3].metric("KPI 快照数", summary["kpi_snapshot_count"])
    summary_cols_2[4].metric("今日异常", summary.get("exception_total_count", 0))
    summary_cols_2[5].metric("阻塞未处理", summary.get("exception_critical_open_count", 0))
    if summary.get("exception_most_affected_count", 0) > 0:
        st.caption(
            f"异常最多员工：{summary.get('exception_most_affected_employee', '-')}（{summary.get('exception_most_affected_count', 0)} 条）"
        )

    if summary["top_keywords"]:
        keyword_text = "、".join(f"{k} ({v})" for k, v in summary["top_keywords"].items())
        st.caption(f"今日高频关键词：{keyword_text}")
    else:
        st.caption("今日暂无可用于展示的关键词分布。")

    st.markdown("---")
    st.subheader("2) 动态 KPI 判断汇报")
    st.caption("这里会明确告诉老板：今天为什么这样定 KPI，以及这个判断参考了哪些资源和风险信号。")

    if not employee_profiles:
        st.info("当前没有电子员工，请先到“员工管理”页面创建员工。")
    elif not snapshots:
        st.info("当前日期还没有生成员工 KPI 判断。点击上方按钮即可生成。")
    else:
        snapshot_map = {
            str(item.get("employee_id", "")): item
            for item in snapshots
        }
        for employee in employee_profiles:
            employee_id = str(employee.get("id", ""))
            snapshot = snapshot_map.get(employee_id)
            with st.container():
                st.markdown(f"### {employee.get('name', '-')}")
                head_cols = st.columns([1.1, 1, 1, 1.4])
                head_cols[0].markdown(
                    f"**类型**：{_employee_type_label(employee.get('employee_type', 'seeding'))}\n\n"
                    f"**状态**：{_employee_status_label(employee.get('status', 'active'))}"
                )
                if snapshot:
                    head_cols[1].metric("今日建议抓取", snapshot.get("target_crawl_count", 0))
                    head_cols[2].metric("今日建议评论", snapshot.get("target_comment_count", 0))
                    head_cols[3].markdown(f"**风险等级**：{_risk_level_markdown(snapshot.get('risk_level', 'medium'))}")
                else:
                    head_cols[1].metric("今日建议抓取", "-")
                    head_cols[2].metric("今日建议评论", "-")
                    head_cols[3].markdown("**风险等级**：-")

                if str(employee.get("employee_type", "")) != "strategy_lead":
                    st.markdown(f"**目标描述**：{employee.get('goal_description', '-')}")
                st.markdown(f"**工作时间**：{_format_work_time_blocks(employee.get('work_time_blocks', []))}")

                if not snapshot:
                    st.warning("该员工当前日期还没有生成 KPI 判断。")
                    st.divider()
                    continue

                source_snapshot = snapshot.get("source_snapshot", {})
                resource_summary = source_snapshot.get("resource_summary", {})
                activity_summary = source_snapshot.get("activity_summary", {})
                bound_prompt = source_snapshot.get("bound_prompt", {})

                evidence_cols = st.columns(4)
                evidence_cols[0].metric("绑定爬取账号", resource_summary.get("assigned_crawl_account_count", 0))
                evidence_cols[1].metric(
                    "活跃评论账号",
                    f"{resource_summary.get('active_comment_account_count', 0)} / {resource_summary.get('assigned_comment_account_count', 0)}",
                )
                evidence_cols[2].metric(
                    "评论日上限",
                    resource_summary.get("comment_daily_capacity_upper_bound", 0),
                )
                evidence_cols[3].metric(
                    "失败信号",
                    activity_summary.get("task_failed_count", 0) + activity_summary.get("failed_comment_count", 0),
                )

                reason_cols = st.columns([1.5, 1])
                with reason_cols[0]:
                    st.markdown("**为什么这么定 KPI**")
                    st.write(snapshot.get("reasoning", "-"))
                with reason_cols[1]:
                    strategy_text = "、".join(
                        f"{item.get('name', '-')}" f"({item.get('active_window', '-')})"
                        for item in resource_summary.get("strategy_packages", [])
                    ) or "未绑定"
                    st.markdown("**这次判断参考了哪些输入**")
                    st.markdown(
                        f"- 爬取账号数：{resource_summary.get('assigned_crawl_account_count', 0)}\n"
                        f"- 评论账号数：{resource_summary.get('active_comment_account_count', 0)}\n"
                        f"- 策略包：{strategy_text}\n"
                        f"- 今日任务失败：{activity_summary.get('task_failed_count', 0)}\n"
                        f"- 今日评论失败：{activity_summary.get('failed_comment_count', 0)}"
                    )

                detail_cols = st.columns(2)
                with detail_cols[0]:
                    st.markdown(
                        f"**今日运营概况**：创建任务 {activity_summary.get('task_created_count', 0)} 个，"
                        f"完成 {activity_summary.get('task_completed_count', 0)} 个，"
                        f"已收录笔记 {activity_summary.get('actual_note_count', 0)} 条，"
                        f"累计互动量 {activity_summary.get('interaction_total', 0)}。"
                    )
                    if activity_summary.get("top_keywords"):
                        st.caption(
                            "员工相关关键词："
                            + "、".join(
                                f"{k} ({v})"
                                for k, v in activity_summary["top_keywords"].items()
                            )
                        )
                with detail_cols[1]:
                    st.markdown(
                        f"**调度边界**：工作窗口 {source_snapshot.get('work_summary', {}).get('work_window_summary', '未设置')}，"
                        f"评论小时上限 {resource_summary.get('comment_hourly_capacity_upper_bound', 0)}，"
                        f"最近评论事件 {activity_summary.get('recent_comment_event_time') or '暂无'}。"
                    )
                    st.caption(
                        f"Prompt：{bound_prompt.get('prompt_name', '-')}"
                        f" | 模型：{snapshot.get('llm_model', '-')}"
                        f" | Token：{_format_token_usage(snapshot.get('total_tokens', 0))}"
                        f" | 更新时间：{snapshot.get('updated_at', '-')}"
                    )
                st.divider()

    st.markdown("---")
    st.subheader("3) 老板日报")
    if not boss_report:
        st.info("当前日期还没有老板日报。点击上方按钮即可生成。")
    else:
        report_meta_cols = st.columns(4)
        report_meta_cols[0].metric("生成员工数", boss_report.get("employee_count", 0))
        report_meta_cols[1].metric("报告模型", boss_report.get("llm_model", "-"))
        report_meta_cols[2].metric("报告 Token", _format_token_usage(boss_report.get("total_tokens", 0)))
        report_meta_cols[3].metric("最后更新时间", boss_report.get("updated_at", "-"))

        st.markdown(boss_report.get("summary_markdown", ""))

        summary_json = boss_report.get("summary", {})
        with st.expander("查看结构化日报详情", expanded=False):
            if summary_json.get("headline"):
                st.markdown(f"**标题**：{summary_json.get('headline')}")
            risks = summary_json.get("risks") or []
            next_actions = summary_json.get("next_actions") or []
            employee_highlights = summary_json.get("employee_highlights") or []
            if risks:
                st.markdown("**主要风险**")
                for item in risks:
                    st.markdown(f"- {item}")
            if next_actions:
                st.markdown("**明日建议**")
                for item in next_actions:
                    st.markdown(f"- {item}")
            if employee_highlights:
                highlight_rows = []
                for item in employee_highlights:
                    highlight_rows.append(
                        {
                            "员工": item.get("employee_name", "-"),
                            "摘要": item.get("summary", "-"),
                            "风险等级": _risk_level_label(item.get("risk_level", "medium")),
                        }
                    )
                st.dataframe(pd.DataFrame(highlight_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("4) 分析建议")
    if not strategy_lead_reports:
        st.info("当前没有分析员工。你可以在“员工管理”创建类型为“分析”的员工。")
    else:
        for report in strategy_lead_reports:
            recommendation = report.get("recommendation", {})
            context = report.get("context", {})
            st.markdown(f"### {report.get('employee_name', '-')}")
            lead_cols = st.columns(4)
            lead_cols[0].metric("覆盖员工数", context.get("target_employee_count", 0))
            lead_cols[1].metric("帖子点赞总量", context.get("post_summary", {}).get("post_like_total", 0))
            lead_cols[2].metric("评论点赞总量", context.get("comment_summary", {}).get("verified_comment_like_total", 0))
            lead_cols[3].metric("阻塞异常", context.get("exception_summary", {}).get("critical_open_count", 0))

            st.markdown("**最优策略**")
            for item in recommendation.get("best_tactics", []):
                st.markdown(f"- {item}")
            st.markdown("**待优化策略**")
            for item in recommendation.get("underperforming_tactics", []):
                st.markdown(f"- {item}")
            st.markdown("**模板建议**")
            for item in recommendation.get("template_suggestions", []):
                st.markdown(f"- {item}")
            st.markdown("**时间段建议**")
            for item in recommendation.get("time_window_suggestions", []):
                st.markdown(f"- {item}")
            st.markdown("**目标帖子建议**")
            for item in recommendation.get("target_post_type_suggestions", []):
                st.markdown(f"- {item}")
            st.markdown("**风险控制建议**")
            for item in recommendation.get("risk_controls", []):
                st.markdown(f"- {item}")
            st.divider()

    st.markdown("---")
    st.subheader("5) 今日操作流水账")
    if not timeline_rows:
        st.info("当前日期暂无可展示的操作流水账。")
    else:
        st.dataframe(pd.DataFrame(timeline_rows), use_container_width=True, hide_index=True)

if current_page == "笔记收录":
    st.header("创建新任务")
    if not runtime_api_key and not str(config.LIST_FILTER_API_KEY or "").strip():
        st.caption("提示：当前未配置 LIST_FILTER_API_KEY。若填写“爬取过滤条件”，任务会记录条件但不会实际执行 AI 判定。")
    
    with st.form("create_task_form"):
        col1, col2 = st.columns(2)
        with col1:
            task_keyword = st.text_input("搜索关键词", "洗面奶测评")
            task_sort = st.selectbox("排序方式", ["综合排序", "最热", "最新"])
            
            # Account selection
            accounts = load_accounts()
            account_options = ["默认 (扫码)"] + [acc['name'] for acc in accounts]
            default_account_idx = 1 if len(account_options) > 1 else 0
            task_account = st.selectbox("选择爬取账号", account_options, index=default_account_idx)
            st.caption("说明：网页端能浏览/评论，不代表爬虫搜索接口一定可用。首次建议用“默认(扫码)”完成一次人工登录验证。")
            low_risk_mode = st.checkbox(
                "低风险模式（推荐）",
                value=True,
                help="自动降低请求强度：关闭评论抓取、延长间隔、保持单并发，降低触发风控概率。",
            )
            
        with col2:
            task_count = st.number_input("爬取数量", min_value=1, max_value=1000, value=100, step=1)
            task_date_range = st.date_input("发布时间范围 (仅作为记录，需配合'最新'排序使用)", [])
            task_filter_condition = st.text_area(
                "爬取过滤条件（AI 粗筛，可选）",
                value="",
                placeholder="示例：只保留真正与自行车整车测评相关的帖子，排除配件、周边、无关广告。",
                help="在搜索列表页先让 AI 判断是否相关；仅保留通过判定的候选帖再抓取详情。",
            )
            
        submitted = st.form_submit_button("创建任务")
        if submitted:
            start_date = task_date_range[0] if len(task_date_range) > 0 else None
            end_date = task_date_range[1] if len(task_date_range) > 1 else None
            available, reason = has_available_login_state(task_account)
            if not available:
                st.error(reason)
            else:
                default_scan_warning = get_default_scan_login_warning(task_account)
                if default_scan_warning:
                    st.warning(default_scan_warning)
                task_id = create_task(
                    task_keyword,
                    task_sort,
                    task_count,
                    start_date,
                    end_date,
                    task_account,
                    task_filter_condition,
                    low_risk_mode,
                )
                st.success(
                    f"任务创建成功（任务ID: {task_id}）。请在任务列表点击“▶️”启动，启动前会自动做浏览器自检。"
                )
                st.rerun()
            
    st.markdown("---")
    render_task_list_fragment()

if current_page == "笔记展示":
    st.header("笔记展示")

    # Load data
    try:
        df = load_data()
        
        if not df.empty:
            # Prepare fields for filtering/sorting.
            df = df.copy()
            df["互动量"] = (
                df["liked_count"].apply(parse_metric_value)
                + df["collected_count"].apply(parse_metric_value)
                + df["comment_count"].apply(parse_metric_value)
            )
            df["收录时间_dt"] = df["add_ts"].apply(to_datetime_from_ts)
            df["收录时间_dt"] = df["收录时间_dt"].where(df["收录时间_dt"].notna(), df["last_modify_ts"].apply(to_datetime_from_ts))
            df["发布时间"] = df["time"].apply(format_timestamp)

            # Top search/filter bar.
            st.subheader("搜筛条件")
            if "note_filter_keyword" not in st.session_state:
                st.session_state.note_filter_keyword = ""
            if "note_filter_logic" not in st.session_state:
                st.session_state.note_filter_logic = "OR"
            if "note_filter_status" not in st.session_state:
                st.session_state.note_filter_status = "全部"
            if "note_filter_sort" not in st.session_state:
                st.session_state.note_filter_sort = "收录时间(新->旧)"
            if "note_filters_reset_requested" not in st.session_state:
                st.session_state.note_filters_reset_requested = False

            source_keyword_scope = str(st.session_state.get("view_source_keyword", "") or "").strip()
            if source_keyword_scope:
                st.info(f"当前正在查看任务关键词「{source_keyword_scope}」的结果。")
                if st.button("退出任务结果视图", key="clear_view_source_keyword"):
                    st.session_state.view_source_keyword = ""
                    st.rerun()

            valid_collect_time = df["收录时间_dt"].dropna()
            min_collect_date, max_collect_date = None, None
            if not valid_collect_time.empty:
                min_collect_date = valid_collect_time.min().date()
                max_collect_date = valid_collect_time.max().date()
                if "note_filter_collect_date_range" not in st.session_state:
                    st.session_state.note_filter_collect_date_range = (min_collect_date, max_collect_date)
            else:
                st.session_state.note_filter_collect_date_range = ()

            min_interaction = int(df["互动量"].min()) if not df.empty else 0
            max_interaction = int(df["互动量"].max()) if not df.empty else 0
            if "note_filter_interaction_range" not in st.session_state:
                st.session_state.note_filter_interaction_range = (min_interaction, max_interaction)

            # Clamp interaction filter in case data range changed.
            i_min, i_max = st.session_state.note_filter_interaction_range
            i_min = max(min_interaction, min(i_min, max_interaction))
            i_max = max(min_interaction, min(i_max, max_interaction))
            if i_min > i_max:
                i_min, i_max = min_interaction, max_interaction
            st.session_state.note_filter_interaction_range = (i_min, i_max)

            # Avoid mutating widget-backed keys after widgets are instantiated.
            if st.session_state.note_filters_reset_requested:
                st.session_state.note_filter_keyword = ""
                st.session_state.note_filter_logic = "OR"
                st.session_state.note_filter_status = "全部"
                st.session_state.note_filter_sort = "收录时间(新->旧)"
                if min_collect_date and max_collect_date:
                    st.session_state.note_filter_collect_date_range = (min_collect_date, max_collect_date)
                else:
                    st.session_state.note_filter_collect_date_range = ()
                st.session_state.note_filter_interaction_range = (min_interaction, max_interaction)
                st.session_state.note_filters_reset_requested = False

            # Sync one-time keyword from "笔记收录"页的“查看结果”.
            if st.session_state.view_keyword:
                st.session_state.note_filter_keyword = st.session_state.view_keyword
                st.session_state.view_keyword = ""

            r1, r2, r3, r4, r5 = st.columns([2.2, 1.1, 1.2, 1.6, 1.1])
            with r1:
                keyword_filter = st.text_input(
                    "关键词",
                    key="note_filter_keyword",
                    placeholder="支持多词，空格分隔（示例：洗面奶 敏感肌）"
                )
            with r2:
                keyword_logic = st.selectbox("关键词逻辑", ["OR", "AND"], key="note_filter_logic")
            with r3:
                status_filter = st.selectbox("笔记状态", ["全部", "Uncommented", "Commented"], key="note_filter_status")
            with r4:
                sort_option = st.selectbox(
                    "排序方式",
                    ["收录时间(新->旧)", "互动量(高->低)", "互动量(低->高)"],
                    key="note_filter_sort"
                )
            with r5:
                st.write("")
                if st.button("重置筛选", use_container_width=True):
                    st.session_state.note_filters_reset_requested = True
                    st.rerun()

            if min_collect_date and max_collect_date:
                collect_date_range = st.date_input(
                    "收录时间",
                    min_value=min_collect_date,
                    max_value=max_collect_date,
                    key="note_filter_collect_date_range"
                )
            else:
                collect_date_range = ()
                st.caption("暂无可用收录时间")

            if min_interaction == max_interaction:
                interaction_range = (min_interaction, max_interaction)
                st.caption(f"互动量范围：{min_interaction:,}（当前数据无波动）")
            else:
                interaction_range = st.slider(
                    "互动量范围（点赞 + 收藏 + 评论）",
                    min_value=min_interaction,
                    max_value=max_interaction,
                    key="note_filter_interaction_range"
                )

            # Apply filters
            if source_keyword_scope:
                df = df[df["source_keyword"].fillna("").astype(str) == source_keyword_scope]

            if keyword_filter:
                tokens = [t for t in re.split(r"[\s,，、]+", keyword_filter.strip()) if t]
                if tokens:
                    searchable_text = (
                        df["title"].fillna("").astype(str) + " "
                        + df["nickname"].fillna("").astype(str) + " "
                        + df["source_keyword"].fillna("").astype(str) + " "
                        + df["desc"].fillna("").astype(str)
                    )
                    if keyword_logic == "AND":
                        keyword_mask = pd.Series(True, index=df.index)
                        for token in tokens:
                            keyword_mask = keyword_mask & searchable_text.str.contains(re.escape(token), case=False, na=False)
                    else:
                        keyword_mask = pd.Series(False, index=df.index)
                        for token in tokens:
                            keyword_mask = keyword_mask | searchable_text.str.contains(re.escape(token), case=False, na=False)
                    df = df[keyword_mask]
            
            if status_filter != "全部":
                df = df[df['comment_status'] == status_filter]

            # Filter by collect date range.
            if "collect_date_range" in locals() and collect_date_range:
                if isinstance(collect_date_range, tuple) and len(collect_date_range) == 2:
                    start_date, end_date = collect_date_range
                else:
                    start_date = end_date = collect_date_range
                if start_date and end_date:
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    df = df[df["收录时间_dt"].notna()]
                    df = df[(df["收录时间_dt"] >= start_dt) & (df["收录时间_dt"] <= end_dt)]

            # Filter by interaction range.
            df = df[
                (df["互动量"] >= interaction_range[0]) &
                (df["互动量"] <= interaction_range[1])
            ]

            # Apply sorting.
            if sort_option == "互动量(高->低)":
                df = df.sort_values(by="互动量", ascending=False)
            elif sort_option == "互动量(低->高)":
                df = df.sort_values(by="互动量", ascending=True)
            else:
                df = df.sort_values(by="收录时间_dt", ascending=False, na_position="last")
            
            # Display metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("笔记总数", len(df))

            total_likes = df['liked_count'].apply(parse_metric_value).sum()
            col2.metric("总点赞数", f"{int(total_likes):,}")
            col3.metric("总互动量", f"{int(df['互动量'].sum()):,}")
            
            # Calculate Publish Duration
            current_ts = int(time.time() * 1000)
            def calc_duration(ts):
                if not ts: return "未知"
                try:
                    diff_ms = current_ts - int(ts)
                    diff_days = diff_ms // (1000 * 60 * 60 * 24)
                    if diff_days > 0:
                        return f"{diff_days}天前"
                    diff_hours = diff_ms // (1000 * 60 * 60)
                    if diff_hours > 0:
                        return f"{diff_hours}小时前"
                    return "刚刚"
                except:
                    return "未知"
            
            df['发布时长'] = df['time'].apply(calc_duration)
            
            # Main table
            st.subheader("笔记列表")
            
            # Header Row
            h1, h2, h3, h4, h5 = st.columns([3, 2, 2, 1, 1.5])
            h1.markdown("**笔记信息**")
            h2.markdown("**博主 / 发布时间**")
            h3.markdown("**互动指标 (点赞/收藏/评论)**")
            h4.markdown("**状态**")
            h5.markdown("**操作**")
            st.divider()
            
            for _, note in df.iterrows():
                with st.container():
                    c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 1, 1.5])
                    
                    # Note Info
                    with c1:
                        st.markdown(f"**{note['title']}**")
                        st.markdown(f"🔗 [查看原文]({note['note_url']})")
                        
                    # Author Info
                    with c2:
                        st.write(note['nickname'])
                        st.caption(f"发布时间：{note.get('发布时间', '')}")
                        st.caption(note['发布时长'])
                        if pd.notna(note.get("收录时间_dt")):
                            st.caption(f"收录：{note['收录时间_dt'].strftime('%Y-%m-%d %H:%M')}")
                        
                    # Metrics
                    with c3:
                        likes_v = int(parse_metric_value(note['liked_count']))
                        collect_v = int(parse_metric_value(note['collected_count']))
                        comment_v = int(parse_metric_value(note['comment_count']))
                        st.markdown(f"❤️ {likes_v:,}  ⭐ {collect_v:,}  💬 {comment_v:,}")
                        st.caption(f"互动量：{int(note['互动量']):,}")
                        
                    # Status
                    with c4:
                        if note['comment_status'] == "Commented":
                            st.markdown(":blue[已评论]")
                        else:
                            st.markdown(":grey[未评论]")
                            
                    # Actions
                    with c5:
                        ac1, ac2, ac3 = st.columns([1.2, 1.2, 1])
                        if ac1.button("去评论", key=f"btn_comment_{note['note_id']}", type="primary"):
                            open_note_comment_dialog(note['note_id'])
                            st.rerun()

                        # View detail dialog (floating layer)
                        if ac2.button("详情", key=f"btn_detail_{note['note_id']}"):
                            st.session_state.selected_note_id = note['note_id']
                            st.rerun()
                            
                        # Delete Note
                        if ac3.button("🗑️", key=f"btn_del_note_{note['note_id']}"):
                            delete_note(note['note_id'])
                            st.rerun()
                    
                    st.divider()

            # Open detail dialog after render loop.
            selected_note_id = st.session_state.get("selected_note_id")
            if selected_note_id:
                selected_rows = df[df["note_id"] == selected_note_id]
                if not selected_rows.empty:
                    show_note_detail_dialog(selected_rows.iloc[0].to_dict())
                # clear after open to avoid repeated pop on unrelated reruns
                st.session_state.selected_note_id = None

            # Open comment-template dialog after render loop.
            selected_comment_note_id = st.session_state.get("selected_comment_note_id")
            if selected_comment_note_id:
                selected_rows = df[df["note_id"] == selected_comment_note_id]
                if not selected_rows.empty:
                    show_comment_template_dialog(selected_rows.iloc[0].to_dict())
                else:
                    clear_comment_dialog_state()
        else:
            st.info("暂无数据，请在'笔记收录'页创建爬取任务。")
                
    except Exception as e:
        st.error(f"加载数据失败: {e}")
        st.info("请确保数据库文件存在且格式正确。")

