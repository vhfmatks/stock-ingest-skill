#!/usr/bin/env python3
"""Standalone stock ingest runtime (no backend app imports).

Features:
- run/status/db-check/help commands
- run_type supports symbols/prices/fundamental/events/margins/all
- sqlite as source of truth
- env preflight check with guidance (expects exported shell variables)
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sqlite3
import sys
import uuid
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_SQLITE_PATH = "~/.openclaw/workspace/data/stockfinder_standalone.db"
DEFAULT_KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
DEFAULT_DART_BASE_URL = "https://opendart.fss.or.kr/api"

RUN_TYPES = {"symbols", "prices", "fundamental", "financials", "events", "margins", "all"}
RUN_TYPE_TO_CATEGORIES: dict[str, tuple[str, ...]] = {
    "symbols": ("symbols",),
    "prices": ("prices",),
    "fundamental": ("financials",),
    "financials": ("financials",),
    "events": ("events",),
    "margins": ("margins",),
    "all": ("symbols", "prices", "financials", "events", "margins"),
}

PRICES_WINDOWS = {
    "fast": 7,
    "normal": 30,
    "full": None,
}

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS ingest_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL UNIQUE,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,
  command TEXT NOT NULL,
  run_type TEXT NOT NULL,
  scope TEXT NOT NULL,
  source_profile TEXT NOT NULL,
  prices_window TEXT,
  prices_lookback_days INTEGER,
  prices_backfill INTEGER NOT NULL DEFAULT 0,
  symbols_count INTEGER NOT NULL DEFAULT 0,
  processed_symbols INTEGER NOT NULL DEFAULT 0,
  symbol_rows INTEGER NOT NULL DEFAULT 0,
  price_rows INTEGER NOT NULL DEFAULT 0,
  fundamental_rows INTEGER NOT NULL DEFAULT 0,
  event_rows INTEGER NOT NULL DEFAULT 0,
  margin_rows INTEGER NOT NULL DEFAULT 0,
  notes_json TEXT,
  error_message TEXT
);

CREATE TABLE IF NOT EXISTS symbol_universe (
  stock_code TEXT PRIMARY KEY,
  name TEXT,
  market TEXT,
  sector TEXT,
  dart_corp_code TEXT,
  listed_date TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  is_delisted INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_price_ohlcv (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  stock_code TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  candle_at TEXT NOT NULL,
  open_price REAL NOT NULL,
  high_price REAL NOT NULL,
  low_price REAL NOT NULL,
  close_price REAL NOT NULL,
  volume REAL NOT NULL,
  source TEXT NOT NULL,
  as_of TEXT,
  collected_at TEXT NOT NULL,
  raw_payload TEXT,
  UNIQUE(stock_code, timeframe, candle_at, source)
);

CREATE TABLE IF NOT EXISTS raw_fundamental_statement (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  stock_code TEXT NOT NULL,
  report_type TEXT NOT NULL,
  period_yyyymm TEXT NOT NULL,
  report_term TEXT NOT NULL,
  item_key TEXT NOT NULL,
  item_label TEXT NOT NULL,
  item_value REAL,
  unit TEXT,
  currency TEXT,
  source TEXT NOT NULL,
  source_key TEXT NOT NULL,
  collected_at TEXT NOT NULL,
  raw_payload TEXT,
  UNIQUE(stock_code, report_type, period_yyyymm, item_key, source, source_key)
);

CREATE TABLE IF NOT EXISTS raw_event_feed (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  stock_code TEXT,
  event_time TEXT NOT NULL,
  event_type TEXT NOT NULL,
  severity INTEGER NOT NULL,
  headline TEXT NOT NULL,
  summary TEXT,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  collected_at TEXT NOT NULL,
  raw_payload TEXT,
  UNIQUE(source, source_event_id)
);

CREATE TABLE IF NOT EXISTS symbol_margin_policy (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  stock_code TEXT NOT NULL,
  as_of TEXT NOT NULL,
  is_full_margin INTEGER NOT NULL DEFAULT 0,
  margin_rate_pct REAL,
  collection_status TEXT NOT NULL,
  source_note TEXT,
  collected_at TEXT NOT NULL,
  UNIQUE(stock_code, as_of)
);
"""


class SetupError(RuntimeError):
    pass


@dataclass
class SymbolEntry:
    stock_code: str
    name: str | None = None
    market: str | None = None
    dart_corp_code: str | None = None
    listed_date: str | None = None  # YYYYMMDD


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def today() -> date:
    return datetime.now(UTC).date()


def normalize_symbol(value: str) -> str | None:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not digits or len(digits) > 6:
        return None
    return digits.zfill(6)


def to_yyyymmdd(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if re.fullmatch(r"\d{8}", raw):
        return raw
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y%m%d")
    except ValueError:
        return None


def to_iso_date(value: str | None) -> str | None:
    ymd = to_yyyymmdd(value)
    if not ymd:
        return None
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"


def to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.replace(",", "").replace("%", "")
    try:
        return float(raw)
    except ValueError:
        return None


def http_get_json(url: str, headers: dict[str, str] | None = None, timeout: float = 20.0) -> Any:
    req = Request(url=url, method="GET", headers=headers or {"Accept": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} {url}: {body[:300]}") from exc
    except URLError as exc:
        raise RuntimeError(f"URL error {url}: {exc}") from exc


def http_post_json(url: str, payload: dict[str, Any], headers: dict[str, str] | None = None, timeout: float = 20.0) -> Any:
    body = json.dumps(payload).encode("utf-8")
    req_headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    req = Request(url=url, method="POST", headers=req_headers, data=body)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} {url}: {err_body[:300]}") from exc
    except URLError as exc:
        raise RuntimeError(f"URL error {url}: {exc}") from exc


class KisClient:
    def __init__(
        self,
        app_key: str,
        app_secret: str,
        base_url: str,
        timeout: float,
        account_no: str = "",
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.account_no = str(account_no or "").strip()
        self._access_token: str | None = None

    def _token(self) -> str:
        if self._access_token:
            return self._access_token
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        data = http_post_json(
            f"{self.base_url}/oauth2/tokenP",
            payload=payload,
            timeout=self.timeout,
        )
        token = data.get("access_token") if isinstance(data, dict) else None
        if not token:
            raise RuntimeError("KIS access_token 발급 실패")
        self._access_token = str(token)
        return self._access_token

    def _get(self, path: str, params: dict[str, Any], tr_id: str) -> dict[str, Any]:
        query = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
        url = f"{self.base_url}{path}?{query}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }
        data = http_get_json(url=url, headers=headers, timeout=self.timeout)
        return data if isinstance(data, dict) else {}

    def fetch_stock_info(self, symbol: str) -> dict[str, Any]:
        data = self._get(
            "/uapi/domestic-stock/v1/quotations/search-stock-info",
            {"PRDT_TYPE_CD": "300", "PDNO": symbol},
            tr_id="CTPF1002R",
        )
        return data.get("output", {}) if isinstance(data.get("output"), dict) else {}

    def fetch_price_rows(
        self,
        symbol: str,
        timeframe: str,
        date_from: str | None,
        date_to: str | None,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        if timeframe not in {"D", "W", "M", "Y"}:
            timeframe = "D"
        to_date = date_to or today().strftime("%Y%m%d")
        from_date = date_from or (today() - timedelta(days=365)).strftime("%Y%m%d")
        tr_cont = ""
        current_to = to_date
        rows: list[dict[str, Any]] = []
        for _ in range(max_pages):
            data = self._get(
                "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                {
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": symbol,
                    "FID_INPUT_DATE_1": from_date,
                    "FID_INPUT_DATE_2": current_to,
                    "FID_PERIOD_DIV_CODE": timeframe,
                    "FID_ORG_ADJ_PRC": "0",
                },
                tr_id="FHKST03010100",
            )
            if str(data.get("rt_cd")) != "0":
                break
            output = data.get("output2") or data.get("output") or []
            if not isinstance(output, list) or not output:
                break
            oldest: str | None = None
            for raw in output:
                if not isinstance(raw, dict):
                    continue
                candle_at = str(raw.get("stck_bsop_date", "")).strip()
                if not candle_at:
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "candle_at": candle_at,
                        "open": raw.get("stck_oprc", "0"),
                        "high": raw.get("stck_hgpr", "0"),
                        "low": raw.get("stck_lwpr", "0"),
                        "close": raw.get("stck_clpr", "0"),
                        "volume": raw.get("acml_vol", "0"),
                        "provider": "kis",
                        "raw": raw,
                    }
                )
                oldest = candle_at

            # fallback date sliding
            if oldest and oldest > from_date:
                try:
                    current_to = (datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
                except ValueError:
                    break
            else:
                break
            if tr_cont == "M":
                tr_cont = "N"
        return rows

    def fetch_fundamental_rows(self, symbol: str) -> list[dict[str, Any]]:
        endpoints: list[tuple[str, str, str]] = [
            ("/uapi/domestic-stock/v1/finance/financial-ratio", "FHKST66430100", "RATIO"),
            ("/uapi/domestic-stock/v1/finance/balance-sheet", "FHKST66430200", "BS"),
            ("/uapi/domestic-stock/v1/finance/profit-ratio", "FHKST66430300", "IS"),
            ("/uapi/domestic-stock/v1/finance/growth-ratio", "FHKST66430400", "GROWTH"),
            ("/uapi/domestic-stock/v1/finance/other-major-ratios", "FHKST66430500", "ETC"),
        ]
        rows: list[dict[str, Any]] = []
        skip_keys = {"stac_yymm", "acml_tr_pbmn", "acml_ntin", "flet_riml_rt", "self_cptl_rt"}
        for div_cls, term in (("0", "annual"), ("1", "quarterly")):
            for path, tr_id, report_type in endpoints:
                data = self._get(
                    path,
                    {
                        "FID_DIV_CLS_CODE": div_cls,
                        "FID_COND_MRKT_DIV_CODE": "J",
                        "FID_INPUT_ISCD": symbol,
                    },
                    tr_id=tr_id,
                )
                if str(data.get("rt_cd")) != "0":
                    continue
                output = data.get("output")
                if not isinstance(output, list):
                    continue
                for item in output:
                    if not isinstance(item, dict):
                        continue
                    period = str(item.get("stac_yymm", "")).strip()
                    if not re.fullmatch(r"\d{6}", period):
                        continue
                    for key, value in item.items():
                        if key in skip_keys or key == "stac_yymm":
                            continue
                        val = str(value).strip() if value is not None else ""
                        if not val:
                            continue
                        try:
                            num = float(val.replace(",", ""))
                        except ValueError:
                            continue
                        rows.append(
                            {
                                "symbol": symbol,
                                "report_type": report_type,
                                "period_yyyymm": period,
                                "report_term": term,
                                "item_key": key.upper(),
                                "item_label": key.upper(),
                                "item_value": num,
                                "unit": None,
                                "currency": "KRW",
                                "source": "kis",
                                "source_key": f"{symbol}:{report_type}:{period}:{term}:{key}",
                                "raw": item,
                            }
                        )
        return rows

    def fetch_margin_rows(self, symbols: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not symbols:
            return rows

        if not self.account_no or len(self.account_no) < 10:
            return rows

        cano = self.account_no[:8]
        acnt_prdt_cd = self.account_no[8:10]
        for symbol in symbols:
            try:
                data = self._get(
                    "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
                    {
                        "CANO": cano,
                        "ACNT_PRDT_CD": acnt_prdt_cd,
                        "PDNO": symbol,
                        "ORD_UNPR": "0",
                        "ORD_DVSN": "01",
                        "CMA_EVLU_AMT_ICLD_YN": "Y",
                        "OVRS_ICLD_YN": "N",
                    },
                    tr_id="TTTC8908R",
                )
                if str(data.get("rt_cd")) != "0":
                    rows.append(
                        {
                            "symbol": symbol,
                            "margin_rate_pct": None,
                            "is_full_margin": False,
                            "collection_status": "failed",
                            "message": str(data.get("msg1", "조회 실패")).strip() or "조회 실패",
                        }
                    )
                    continue

                data2 = self._get(
                    "/uapi/domestic-stock/v1/trading/intgr-margin",
                    {
                        "CANO": cano,
                        "ACNT_PRDT_CD": acnt_prdt_cd,
                        "PDNO": symbol,
                    },
                    tr_id="TTTC0869R",
                )

                margin_rate_pct: float | None = None
                is_full = False
                if str(data2.get("rt_cd")) == "0":
                    output2 = data2.get("output", {}) if isinstance(data2.get("output"), dict) else {}
                    margin_rate_pct = to_float_or_none(output2.get("acmga_rt"))
                    is_full = bool(margin_rate_pct is not None and margin_rate_pct >= 100.0)

                if margin_rate_pct is not None:
                    message = f"증거금율 {margin_rate_pct}%"
                    status = "collected"
                else:
                    message = "증거금 정보 없음"
                    status = "failed"

                rows.append(
                    {
                        "symbol": symbol,
                        "margin_rate_pct": margin_rate_pct,
                        "is_full_margin": is_full,
                        "collection_status": status,
                        "message": message,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                rows.append(
                    {
                        "symbol": symbol,
                        "margin_rate_pct": None,
                        "is_full_margin": False,
                        "collection_status": "failed",
                        "message": f"조회 실패: {exc}",
                    }
                )
        return rows


def fetch_dart_corp_codes(api_key: str, timeout: float, dart_base_url: str) -> list[SymbolEntry]:
    url = f"{dart_base_url.rstrip('/')}/corpCode.xml?crtfc_key={api_key}"
    req = Request(url=url, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"DART corpCode HTTP {exc.code}: {body[:200]}") from exc
    except URLError as exc:
        raise RuntimeError(f"DART corpCode URL error: {exc}") from exc

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            xml_name = zf.namelist()[0]
            xml_data = zf.read(xml_name)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("DART corpCode ZIP 파싱 실패") from exc

    root = ET.fromstring(xml_data)
    out: list[SymbolEntry] = []
    for node in root.findall("./list"):
        stock_code = normalize_symbol(node.findtext("stock_code", default=""))
        if not stock_code:
            continue
        out.append(
            SymbolEntry(
                stock_code=stock_code,
                name=(node.findtext("corp_name") or "").strip() or None,
                dart_corp_code=(node.findtext("corp_code") or "").strip() or None,
                listed_date=None,
            )
        )
    return out


def fetch_dart_events(
    api_key: str,
    dart_base_url: str,
    corp_code: str,
    begin_date: str,
    end_date: str,
    timeout: float,
) -> list[dict[str, Any]]:
    # https://opendart.fss.or.kr/api/list.json
    query = urlencode(
        {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": begin_date,
            "end_de": end_date,
            "page_count": 100,
        }
    )
    url = f"{dart_base_url.rstrip('/')}/list.json?{query}"
    payload = http_get_json(url=url, timeout=timeout)
    if not isinstance(payload, dict):
        return []
    if payload.get("status") != "000":
        return []
    items = payload.get("list")
    if not isinstance(items, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rcept_no = str(item.get("rcept_no", "")).strip()
        if not rcept_no:
            continue
        rcept_dt = str(item.get("rcept_dt", "")).strip()
        event_time = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:8]}T00:00:00+00:00" if re.fullmatch(r"\d{8}", rcept_dt) else now_iso()
        rows.append(
            {
                "event_time": event_time,
                "event_type": "dart_disclosure",
                "severity": 3,
                "headline": str(item.get("report_nm", "")).strip() or "DART disclosure",
                "summary": str(item.get("flr_nm", "")).strip() or None,
                "source": "dart",
                "source_event_id": rcept_no,
                "raw": item,
            }
        )
    return rows


def connect_sqlite(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    return conn


def upsert_run(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO ingest_runs (
          run_id, started_at, finished_at, status, command, run_type, scope, source_profile,
          prices_window, prices_lookback_days, prices_backfill, symbols_count, processed_symbols,
          symbol_rows, price_rows, fundamental_rows, event_rows, margin_rows, notes_json, error_message
        ) VALUES (
          :run_id, :started_at, :finished_at, :status, :command, :run_type, :scope, :source_profile,
          :prices_window, :prices_lookback_days, :prices_backfill, :symbols_count, :processed_symbols,
          :symbol_rows, :price_rows, :fundamental_rows, :event_rows, :margin_rows, :notes_json, :error_message
        )
        ON CONFLICT(run_id) DO UPDATE SET
          finished_at=excluded.finished_at,
          status=excluded.status,
          processed_symbols=excluded.processed_symbols,
          symbol_rows=excluded.symbol_rows,
          price_rows=excluded.price_rows,
          fundamental_rows=excluded.fundamental_rows,
          event_rows=excluded.event_rows,
          margin_rows=excluded.margin_rows,
          notes_json=excluded.notes_json,
          error_message=excluded.error_message
        """,
        row,
    )


def upsert_symbol(conn: sqlite3.Connection, row: SymbolEntry) -> None:
    conn.execute(
        """
        INSERT INTO symbol_universe (
          stock_code, name, market, sector, dart_corp_code, listed_date, is_active, is_delisted, updated_at
        ) VALUES (?, ?, ?, NULL, ?, ?, 1, 0, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
          name=COALESCE(excluded.name, symbol_universe.name),
          market=COALESCE(excluded.market, symbol_universe.market),
          dart_corp_code=COALESCE(excluded.dart_corp_code, symbol_universe.dart_corp_code),
          listed_date=COALESCE(excluded.listed_date, symbol_universe.listed_date),
          updated_at=excluded.updated_at
        """,
        (
            row.stock_code,
            row.name,
            row.market,
            row.dart_corp_code,
            to_iso_date(row.listed_date),
            now_iso(),
        ),
    )


def upsert_price(conn: sqlite3.Connection, run_id: str, row: dict[str, Any], as_of: str | None) -> None:
    candle_at = to_iso_date(row.get("candle_at"))
    if candle_at is None:
        return
    conn.execute(
        """
        INSERT INTO raw_price_ohlcv (
          run_id, stock_code, timeframe, candle_at, open_price, high_price, low_price, close_price, volume, source,
          as_of, collected_at, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, timeframe, candle_at, source) DO UPDATE SET
          run_id=excluded.run_id,
          open_price=excluded.open_price,
          high_price=excluded.high_price,
          low_price=excluded.low_price,
          close_price=excluded.close_price,
          volume=excluded.volume,
          as_of=excluded.as_of,
          collected_at=excluded.collected_at,
          raw_payload=excluded.raw_payload
        """,
        (
            run_id,
            row.get("symbol"),
            row.get("timeframe", "D"),
            candle_at,
            float(row.get("open", 0) or 0),
            float(row.get("high", 0) or 0),
            float(row.get("low", 0) or 0),
            float(row.get("close", 0) or 0),
            float(row.get("volume", 0) or 0),
            str(row.get("provider", "unknown")),
            as_of,
            now_iso(),
            json.dumps(row.get("raw", {}), ensure_ascii=False),
        ),
    )


def upsert_fundamental(conn: sqlite3.Connection, run_id: str, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO raw_fundamental_statement (
          run_id, stock_code, report_type, period_yyyymm, report_term, item_key, item_label, item_value, unit, currency,
          source, source_key, collected_at, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, report_type, period_yyyymm, item_key, source, source_key) DO UPDATE SET
          run_id=excluded.run_id,
          item_value=excluded.item_value,
          item_label=excluded.item_label,
          unit=excluded.unit,
          currency=excluded.currency,
          collected_at=excluded.collected_at,
          raw_payload=excluded.raw_payload
        """,
        (
            run_id,
            row.get("symbol"),
            row.get("report_type"),
            row.get("period_yyyymm"),
            row.get("report_term"),
            row.get("item_key"),
            row.get("item_label"),
            row.get("item_value"),
            row.get("unit"),
            row.get("currency"),
            row.get("source", "unknown"),
            row.get("source_key"),
            now_iso(),
            json.dumps(row.get("raw", {}), ensure_ascii=False),
        ),
    )


def upsert_event(conn: sqlite3.Connection, run_id: str, stock_code: str | None, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO raw_event_feed (
          run_id, stock_code, event_time, event_type, severity, headline, summary, source, source_event_id, collected_at, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, source_event_id) DO UPDATE SET
          run_id=excluded.run_id,
          stock_code=excluded.stock_code,
          event_time=excluded.event_time,
          event_type=excluded.event_type,
          severity=excluded.severity,
          headline=excluded.headline,
          summary=excluded.summary,
          collected_at=excluded.collected_at,
          raw_payload=excluded.raw_payload
        """,
        (
            run_id,
            stock_code,
            row.get("event_time"),
            row.get("event_type", "notice"),
            int(row.get("severity", 3)),
            row.get("headline", "event"),
            row.get("summary"),
            row.get("source", "unknown"),
            row.get("source_event_id"),
            now_iso(),
            json.dumps(row.get("raw", {}), ensure_ascii=False),
        ),
    )


def upsert_margin_policy(
    conn: sqlite3.Connection,
    run_id: str,
    stock_code: str,
    as_of: str,
    is_full_margin: bool = False,
    margin_rate_pct: float | None = None,
    collection_status: str = "collected",
    source_note: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO symbol_margin_policy (
          run_id, stock_code, as_of, is_full_margin, margin_rate_pct, collection_status, source_note, collected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, as_of) DO UPDATE SET
          run_id=excluded.run_id,
          is_full_margin=excluded.is_full_margin,
          margin_rate_pct=excluded.margin_rate_pct,
          collection_status=excluded.collection_status,
          source_note=excluded.source_note,
          collected_at=excluded.collected_at
        """,
        (
            run_id,
            stock_code,
            as_of,
            1 if is_full_margin else 0,
            margin_rate_pct,
            collection_status,
            source_note,
            now_iso(),
        ),
    )


def parse_symbols(args: argparse.Namespace) -> list[str]:
    out: list[str] = []
    for raw in list(args.symbol or []):
        s = normalize_symbol(raw)
        if s and s not in out:
            out.append(s)
    for token in str(args.symbols or "").split(","):
        token = token.strip()
        if not token:
            continue
        s = normalize_symbol(token)
        if s and s not in out:
            out.append(s)
    return out


def resolve_symbols(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    notes: list[str],
    dart_api_key: str | None,
) -> list[SymbolEntry]:
    scope = args.scope
    user_symbols = parse_symbols(args)
    if scope == "single":
        if not user_symbols:
            raise SetupError("scope=single 에서는 --symbol 또는 --symbols 입력이 필요합니다.")
        entries = [SymbolEntry(stock_code=s, name=None) for s in user_symbols]
    else:
        entries: list[SymbolEntry] = []
        if dart_api_key:
            entries = fetch_dart_corp_codes(
                api_key=dart_api_key,
                timeout=args.timeout,
                dart_base_url=args.dart_base_url,
            )
            notes.append(f"all scope symbols resolved via DART corpCode: {len(entries)}")
        else:
            rows = conn.execute("SELECT stock_code, name, market, dart_corp_code, listed_date FROM symbol_universe").fetchall()
            entries = [
                SymbolEntry(
                    stock_code=str(r[0]),
                    name=r[1],
                    market=r[2],
                    dart_corp_code=r[3],
                    listed_date=(r[4] or "").replace("-", "") if r[4] else None,
                )
                for r in rows
            ]
            if entries:
                notes.append(f"all scope symbols resolved via sqlite symbol_universe: {len(entries)}")
        if not entries:
            raise SetupError(
                "scope=all 심볼 소스를 찾지 못했습니다. DART_API_KEY를 export 하거나 sqlite symbol_universe를 먼저 채우세요."
            )
        if args.limit_symbols and args.limit_symbols > 0:
            entries = entries[: args.limit_symbols]
            notes.append(f"limit_symbols applied: {len(entries)}")
    return entries


def ensure_exported_env(args: argparse.Namespace) -> tuple[str | None, str | None, str | None]:
    run_type = str(args.run_type).strip().lower()
    categories = RUN_TYPE_TO_CATEGORIES.get(run_type, ())
    source_profile = str(args.source_profile).strip().lower()
    scope = str(args.scope).strip().lower()
    is_dry = bool(args.dry_run)

    need_kis = any(cat in categories for cat in ("prices", "financials", "margins")) and source_profile in {"all", "kis"}
    need_dart = (scope == "all") or ("events" in categories and source_profile in {"all", "dart"})
    missing: list[str] = []
    if not is_dry and need_kis:
        if not (args.kis_app_key or "").strip():
            missing.append("KIS_APP_KEY")
        if not (args.kis_app_secret or "").strip():
            missing.append("KIS_APP_SECRET")
    if not is_dry and "margins" in categories and source_profile in {"all", "kis"}:
        if not (args.kis_account_no or "").strip():
            missing.append("KIS_ACCOUNT_NO")
    if not is_dry and need_dart and not (args.dart_api_key or "").strip():
        missing.append("DART_API_KEY")

    if missing:
        lines = [
            "[SETUP REQUIRED] 로컬 shell 환경변수(export)가 필요합니다.",
            f"누락 키: {' '.join(missing)}",
            "",
            "아래처럼 ~/.bashrc 또는 ~/.profile 에 export 후 source 하세요:",
        ]
        for key in missing:
            lines.append(f"  export {key}=<YOUR_VALUE>")
        lines.extend(
            [
                "  source ~/.bashrc   # 또는 source ~/.profile",
                "",
                "현재 세션 확인:",
            ]
        )
        for key in missing:
            lines.append(f"  printenv {key}")
        lines.append("")
        lines.append("설정 완료 후 동일 명령을 다시 실행하세요.")
        raise SetupError("\n".join(lines))

    return args.kis_app_key, args.kis_app_secret, args.dart_api_key


def derive_price_range(
    args: argparse.Namespace,
    symbol: SymbolEntry,
) -> tuple[str | None, str | None]:
    explicit_from = to_yyyymmdd(args.as_of_from)
    explicit_to = to_yyyymmdd(args.as_of_to)
    if explicit_from or explicit_to:
        return explicit_from, explicit_to

    if args.prices_lookback_days:
        d = int(args.prices_lookback_days)
        to_d = today()
        from_d = to_d - timedelta(days=d)
        return from_d.strftime("%Y%m%d"), to_d.strftime("%Y%m%d")

    if args.prices_window in {"fast", "normal"}:
        days = PRICES_WINDOWS[args.prices_window]
        to_d = today()
        from_d = to_d - timedelta(days=int(days))
        return from_d.strftime("%Y%m%d"), to_d.strftime("%Y%m%d")

    # full/backfill
    if args.prices_window == "full" or args.prices_backfill:
        from_d = symbol.listed_date or "19900101"
        return from_d, today().strftime("%Y%m%d")

    return None, None


def run_ingest(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    sqlite_path = Path(args.sqlite_path).expanduser()
    notes: list[str] = []

    try:
        kis_key, kis_secret, dart_key = ensure_exported_env(args)
    except SetupError as exc:
        return 3, {"ok": False, "run_id": run_id, "status": "setup_required", "error": str(exc)}

    conn = connect_sqlite(sqlite_path)
    row = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": None,
        "status": "running",
        "command": "run",
        "run_type": args.run_type,
        "scope": args.scope,
        "source_profile": args.source_profile,
        "prices_window": args.prices_window,
        "prices_lookback_days": args.prices_lookback_days,
        "prices_backfill": 1 if args.prices_backfill else 0,
        "symbols_count": 0,
        "processed_symbols": 0,
        "symbol_rows": 0,
        "price_rows": 0,
        "fundamental_rows": 0,
        "event_rows": 0,
        "margin_rows": 0,
        "notes_json": "[]",
        "error_message": None,
    }
    upsert_run(conn, row)
    conn.commit()

    if args.dry_run:
        payload = {
            "run_type": args.run_type,
            "scope": args.scope,
            "symbols": parse_symbols(args),
            "source_profile": args.source_profile,
            "timeframes": args.timeframes,
            "prices_window": args.prices_window,
            "prices_lookback_days": args.prices_lookback_days,
            "prices_backfill": args.prices_backfill,
            "as_of_from": args.as_of_from,
            "as_of_to": args.as_of_to,
            "sqlite_path": str(sqlite_path),
            "dry_run": True,
        }
        row.update(
            {
                "finished_at": now_iso(),
                "status": "success",
                "notes_json": json.dumps(["dry-run"], ensure_ascii=False),
            }
        )
        upsert_run(conn, row)
        conn.commit()
        conn.close()
        return 0, {"ok": True, "run_id": run_id, "status": "success", "payload": payload}

    kis_client = None
    if kis_key and kis_secret:
        kis_client = KisClient(
            app_key=kis_key,
            app_secret=kis_secret,
            base_url=args.kis_base_url,
            timeout=args.timeout,
            account_no=args.kis_account_no,
        )

    status = "success"
    error_message: str | None = None
    try:
        categories = RUN_TYPE_TO_CATEGORIES[args.run_type]
        symbols = resolve_symbols(conn, args, notes, dart_api_key=dart_key)
        row["symbols_count"] = len(symbols)

        # symbols stage
        if "symbols" in categories:
            for sym in symbols:
                # single scope and KIS available -> enrich symbol info
                if kis_client and args.scope == "single":
                    try:
                        info = kis_client.fetch_stock_info(sym.stock_code)
                        mket_id = str(info.get("mket_id_cd", "")).strip().upper()
                        if mket_id == "STK":
                            sym.market = "KOSPI"
                        elif mket_id == "KSQ":
                            sym.market = "KOSDAQ"
                        listed = str(info.get("scts_mket_lstg_dt", "")).strip()
                        if re.fullmatch(r"\d{8}", listed):
                            sym.listed_date = listed
                        name = str(info.get("prdt_abrv_name", "")).strip()
                        if name:
                            sym.name = name
                    except Exception as exc:  # noqa: BLE001
                        notes.append(f"symbol enrich failed {sym.stock_code}: {exc}")

                upsert_symbol(conn, sym)
                row["symbol_rows"] += 1
            conn.commit()

        # prices stage
        if "prices" in categories:
            if not kis_client:
                notes.append("prices skipped: KIS_APP_KEY/KIS_APP_SECRET not provided")
            else:
                timeframes = args.timeframes or ["D"]
                for sym in symbols:
                    date_from, date_to = derive_price_range(args, sym)
                    for tf in timeframes:
                        price_rows = kis_client.fetch_price_rows(
                            symbol=sym.stock_code,
                            timeframe=tf,
                            date_from=date_from,
                            date_to=date_to,
                            max_pages=max(1, args.kis_max_price_pages),
                        )
                        for p in price_rows:
                            upsert_price(conn, run_id=run_id, row=p, as_of=to_iso_date(args.as_of_to) or to_iso_date(args.as_of))
                            row["price_rows"] += 1
                    row["processed_symbols"] += 1
                conn.commit()

        # financials stage
        if "financials" in categories:
            if not kis_client:
                notes.append("financials skipped: KIS_APP_KEY/KIS_APP_SECRET not provided")
            else:
                for sym in symbols:
                    f_rows = kis_client.fetch_fundamental_rows(sym.stock_code)
                    for f in f_rows:
                        upsert_fundamental(conn, run_id=run_id, row=f)
                        row["fundamental_rows"] += 1
                conn.commit()

        # events stage (DART)
        if "events" in categories:
            if not dart_key:
                notes.append("events skipped: DART_API_KEY not provided")
            else:
                end_d = to_yyyymmdd(args.as_of_to) or today().strftime("%Y%m%d")
                begin_d = to_yyyymmdd(args.as_of_from) or (today() - timedelta(days=30)).strftime("%Y%m%d")
                for sym in symbols:
                    if not sym.dart_corp_code:
                        continue
                    events = fetch_dart_events(
                        api_key=dart_key,
                        dart_base_url=args.dart_base_url,
                        corp_code=sym.dart_corp_code,
                        begin_date=begin_d,
                        end_date=end_d,
                        timeout=args.timeout,
                    )
                    for event in events:
                        upsert_event(conn, run_id=run_id, stock_code=sym.stock_code, row=event)
                        row["event_rows"] += 1
                conn.commit()

        # margins stage
        if "margins" in categories:
            if args.source_profile not in {"all", "kis"}:
                notes.append("margins skipped: source_profile does not include kis")
            elif not kis_client:
                notes.append("margins skipped: KIS_APP_KEY/KIS_APP_SECRET not provided")
            else:
                as_of = to_iso_date(args.as_of_to) or to_iso_date(args.as_of) or today().isoformat()
                margin_rows = kis_client.fetch_margin_rows([sym.stock_code for sym in symbols])
                collected_count = 0
                failed_count = 0
                for item in margin_rows:
                    stock_code = normalize_symbol(str(item.get("symbol", "")).strip())
                    if not stock_code:
                        continue
                    status_text = str(item.get("collection_status", "collected")).strip().lower() or "collected"
                    if status_text == "collected":
                        collected_count += 1
                    else:
                        failed_count += 1
                    upsert_margin_policy(
                        conn,
                        run_id=run_id,
                        stock_code=stock_code,
                        as_of=as_of,
                        is_full_margin=bool(item.get("is_full_margin")),
                        margin_rate_pct=to_float_or_none(item.get("margin_rate_pct")),
                        collection_status=status_text,
                        source_note=str(item.get("message", "")).strip() or None,
                    )
                    row["margin_rows"] += 1
                conn.commit()
                notes.append(f"margins: collected={collected_count}, failed={failed_count}")

    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error_message = str(exc)
        notes.append(f"error: {error_message}")

    row["status"] = status
    row["finished_at"] = now_iso()
    row["notes_json"] = json.dumps(notes, ensure_ascii=False)
    row["error_message"] = error_message
    upsert_run(conn, row)
    conn.commit()
    conn.close()

    payload = {
        "ok": status == "success",
        "run_id": run_id,
        "status": status,
        "run_type": args.run_type,
        "scope": args.scope,
        "source_profile": args.source_profile,
        "prices_window": args.prices_window,
        "prices_lookback_days": args.prices_lookback_days,
        "prices_backfill": args.prices_backfill,
        "symbols_count": row["symbols_count"],
        "processed_symbols": row["processed_symbols"],
        "row_counts": {
            "symbol_universe": row["symbol_rows"],
            "raw_price_ohlcv": row["price_rows"],
            "raw_fundamental_statement": row["fundamental_rows"],
            "raw_event_feed": row["event_rows"],
            "symbol_margin_policy": row["margin_rows"],
        },
        "notes": notes,
        "error": error_message,
        "sqlite_path": str(sqlite_path),
        "started_at": started_at,
        "finished_at": row["finished_at"],
        "standalone": True,
    }
    return (0 if status == "success" else 1), payload


def get_status(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT run_id, started_at, finished_at, status, run_type, scope, source_profile,
               prices_window, prices_lookback_days, prices_backfill, symbols_count, processed_symbols,
               symbol_rows, price_rows, fundamental_rows, event_rows, margin_rows, notes_json, error_message
        FROM ingest_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        return {"ok": False, "error": f"run_id not found: {run_id}"}
    return {
        "ok": True,
        "run_id": row[0],
        "started_at": row[1],
        "finished_at": row[2],
        "status": row[3],
        "run_type": row[4],
        "scope": row[5],
        "source_profile": row[6],
        "prices_window": row[7],
        "prices_lookback_days": row[8],
        "prices_backfill": bool(row[9]),
        "symbols_count": row[10],
        "processed_symbols": row[11],
        "row_counts": {
            "symbol_universe": row[12],
            "raw_price_ohlcv": row[13],
            "raw_fundamental_statement": row[14],
            "raw_event_feed": row[15],
            "symbol_margin_policy": row[16],
        },
        "notes": json.loads(row[17]) if row[17] else [],
        "error": row[18],
    }


def db_check(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    base = get_status(conn, run_id)
    if not base.get("ok"):
        return base
    counts = {}
    for table in ("raw_price_ohlcv", "raw_fundamental_statement", "raw_event_feed", "symbol_margin_policy"):
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = ?", (run_id,)).fetchone()[0]
        counts[table] = int(cnt)
    base["db_check"] = counts
    return base


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="stock-ingest standalone runtime (sqlite)")
    p.add_argument("--json", action="store_true", default=False, dest="json_output")
    p.add_argument("--sqlite-path", default=DEFAULT_SQLITE_PATH)
    p.add_argument("--timeout", type=float, default=20.0)
    p.add_argument("--kis-base-url", default=None)
    p.add_argument("--dart-base-url", default=DEFAULT_DART_BASE_URL)
    p.add_argument("--kis-max-price-pages", type=int, default=3)

    sub = p.add_subparsers(dest="command")

    run = sub.add_parser("run", help="run standalone ingest")
    run.add_argument("--run-type", default="all", choices=sorted(RUN_TYPES))
    run.add_argument("--scope", choices=["single", "all"], default="single")
    run.add_argument("--symbol", action="append", default=[])
    run.add_argument("--symbols", default="")
    run.add_argument("--source-profile", choices=["all", "kis", "dart"], default="all")
    run.add_argument("--timeframes", nargs="*", default=["D"], choices=["D", "W", "M", "Y"])
    run.add_argument("--as-of", default=None)
    run.add_argument("--as-of-from", default=None)
    run.add_argument("--as-of-to", default=None)
    run.add_argument("--prices-window", choices=["fast", "normal", "full"], default="fast")
    run.add_argument("--prices-lookback-days", type=int, default=None)
    run.add_argument("--prices-backfill", action="store_true", default=False)
    run.add_argument("--limit-symbols", type=int, default=None)
    run.add_argument("--dry-run", action="store_true", default=False)

    st = sub.add_parser("status", help="show run status from sqlite")
    st.add_argument("run_id")

    dbc = sub.add_parser("db-check", help="show run row counts from sqlite")
    dbc.add_argument("run_id")

    sub.add_parser("help", help="show help")

    return p


def print_payload(payload: dict[str, Any], json_mode: bool) -> None:
    if json_mode:
        print(json.dumps(payload, ensure_ascii=False))
        return
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    args.kis_base_url = args.kis_base_url or os.environ.get("KIS_BASE_URL", DEFAULT_KIS_BASE_URL)
    args.kis_app_key = os.environ.get("KIS_APP_KEY", "")
    args.kis_app_secret = os.environ.get("KIS_APP_SECRET", "")
    args.kis_account_no = os.environ.get("KIS_ACCOUNT_NO", "")
    args.dart_api_key = os.environ.get("DART_API_KEY", "")

    command = args.command or "help"
    sqlite_path = Path(args.sqlite_path).expanduser()
    conn = connect_sqlite(sqlite_path)
    conn.close()

    if command == "help":
        parser.print_help()
        return 0

    if command == "run":
        code, payload = run_ingest(args)
        print_payload(payload, args.json_output)
        return code

    conn = connect_sqlite(sqlite_path)
    try:
        if command == "status":
            payload = get_status(conn, args.run_id)
            print_payload(payload, args.json_output)
            return 0 if payload.get("ok") else 1
        if command == "db-check":
            payload = db_check(conn, args.run_id)
            print_payload(payload, args.json_output)
            return 0 if payload.get("ok") else 1
    finally:
        conn.close()

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
