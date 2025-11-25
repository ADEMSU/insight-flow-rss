from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, date
from typing import Dict, Any
from urllib.parse import urlparse
from loguru import logger


class StatsCollector:
    _instance: "StatsCollector" | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Category/subcategory issues
            cls._instance.sub_mismatch_by_category = defaultdict(int)
            cls._instance.invalid_category_reasons = defaultdict(int)
            cls._instance.missing_category_or_subcategory = 0
            # RSS issues
            cls._instance.rss_issues_total = 0
            cls._instance.rss_issues_by_type = defaultdict(int)
            cls._instance.rss_issues_by_domain = defaultdict(int)
        return cls._instance

    # ---------------- Category metrics ----------------
    def record_sub_mismatch(self, category: str, subcategory: str) -> None:
        key = category or "<empty>"
        self.sub_mismatch_by_category[key] += 1

    def record_invalid_category(self, reason: str) -> None:
        key = (reason or "<empty>")[:200]
        self.invalid_category_reasons[key] += 1

    def record_missing_category(self) -> None:
        self.missing_category_or_subcategory += 1

    # ---------------- RSS metrics ---------------------
    def record_rss_issue(self, source_name: str, url: str, issue_type: str, details: str = "") -> None:
        self.rss_issues_total += 1
        itype = (issue_type or "unknown").lower()
        self.rss_issues_by_type[itype] += 1
        try:
            domain = urlparse(url).netloc.lower()
        except Exception:
            domain = ""
        if domain:
            self.rss_issues_by_domain[domain] += 1

    # ---------------- Flush to file -------------------
    def _ensure_dir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def _monthly_path(self, logs_dir: str, dt: date) -> str:
        month_key = dt.strftime("%Y-%m")
        return os.path.join(logs_dir, f"stats_{month_key}.json")

    def _build_day_payload(self) -> Dict[str, Any]:
        return {
            "categories": {
                "sub_mismatch_by_category": dict(self.sub_mismatch_by_category),
                "invalid_category_reasons": dict(self.invalid_category_reasons),
                "missing_category_or_subcategory": self.missing_category_or_subcategory,
            },
            "rss": {
                "total": self.rss_issues_total,
                "by_type": dict(self.rss_issues_by_type),
                "by_domain": dict(self.rss_issues_by_domain),
            },
        }

    def _aggregate_week(self, days: Dict[str, Any], iso_year: int, iso_week: int) -> Dict[str, Any]:
        # Sum all days matching iso_year-week in provided days map
        agg = {
            "categories": {
                "sub_mismatch_by_category": defaultdict(int),
                "invalid_category_reasons": defaultdict(int),
                "missing_category_or_subcategory": 0,
            },
            "rss": {"total": 0, "by_type": defaultdict(int), "by_domain": defaultdict(int)},
        }
        for dstr, payload in days.items():
            try:
                d = datetime.strptime(dstr, "%Y-%m-%d").date()
            except Exception:
                continue
            y, w, _ = d.isocalendar()
            if y != iso_year or w != iso_week:
                continue
            cat = payload.get("categories", {})
            for k, v in cat.get("sub_mismatch_by_category", {}).items():
                agg["categories"]["sub_mismatch_by_category"][k] += int(v)
            for k, v in cat.get("invalid_category_reasons", {}).items():
                agg["categories"]["invalid_category_reasons"][k] += int(v)
            agg["categories"]["missing_category_or_subcategory"] += int(cat.get("missing_category_or_subcategory", 0))

            rss = payload.get("rss", {})
            agg["rss"]["total"] += int(rss.get("total", 0))
            for k, v in rss.get("by_type", {}).items():
                agg["rss"]["by_type"][k] += int(v)
            for k, v in rss.get("by_domain", {}).items():
                agg["rss"]["by_domain"][k] += int(v)

        # Cast defaultdicts to dicts
        agg["categories"]["sub_mismatch_by_category"] = dict(agg["categories"]["sub_mismatch_by_category"])
        agg["categories"]["invalid_category_reasons"] = dict(agg["categories"]["invalid_category_reasons"])
        agg["rss"]["by_type"] = dict(agg["rss"]["by_type"])
        agg["rss"]["by_domain"] = dict(agg["rss"]["by_domain"])
        return agg

    def _aggregate_month(self, days: Dict[str, Any], year: int, month: int) -> Dict[str, Any]:
        agg = {
            "categories": {
                "sub_mismatch_by_category": defaultdict(int),
                "invalid_category_reasons": defaultdict(int),
                "missing_category_or_subcategory": 0,
            },
            "rss": {"total": 0, "by_type": defaultdict(int), "by_domain": defaultdict(int)},
        }
        for dstr, payload in days.items():
            try:
                d = datetime.strptime(dstr, "%Y-%m-%d").date()
            except Exception:
                continue
            if d.year != year or d.month != month:
                continue
            cat = payload.get("categories", {})
            for k, v in cat.get("sub_mismatch_by_category", {}).items():
                agg["categories"]["sub_mismatch_by_category"][k] += int(v)
            for k, v in cat.get("invalid_category_reasons", {}).items():
                agg["categories"]["invalid_category_reasons"][k] += int(v)
            agg["categories"]["missing_category_or_subcategory"] += int(cat.get("missing_category_or_subcategory", 0))

            rss = payload.get("rss", {})
            agg["rss"]["total"] += int(rss.get("total", 0))
            for k, v in rss.get("by_type", {}).items():
                agg["rss"]["by_type"][k] += int(v)
            for k, v in rss.get("by_domain", {}).items():
                agg["rss"]["by_domain"][k] += int(v)

        agg["categories"]["sub_mismatch_by_category"] = dict(agg["categories"]["sub_mismatch_by_category"])
        agg["categories"]["invalid_category_reasons"] = dict(agg["categories"]["invalid_category_reasons"])
        agg["rss"]["by_type"] = dict(agg["rss"]["by_type"])
        agg["rss"]["by_domain"] = dict(agg["rss"]["by_domain"])
        return agg

    def flush_monthly(self, logs_dir: str = "logs", dt: date | None = None) -> None:
        dt = dt or datetime.now().date()
        self._ensure_dir(logs_dir)
        path = self._monthly_path(logs_dir, dt)

        # load existing
        data: Dict[str, Any] = {"days": {}, "weekly": {}, "month_total": {}}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logger.warning(f"StatsCollector: cannot read stats file {path}: {e}")

        # update day entry
        day_key = dt.strftime("%Y-%m-%d")
        data.setdefault("days", {})[day_key] = self._build_day_payload()

        # recompute weekly aggregate for this ISO week
        iso_year, iso_week, _ = dt.isocalendar()
        week_key = f"{iso_year}-W{iso_week:02d}"
        data["weekly"][week_key] = self._aggregate_week(data["days"], iso_year, iso_week)

        # recompute month-to-date aggregate
        data["month_total"] = self._aggregate_month(data["days"], dt.year, dt.month)

        # write back
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

        logger.info(f"StatsCollector: flushed stats for {day_key} → {path}")

    # ---------------- Reset counters -----------------
    def reset(self) -> None:
        self.sub_mismatch_by_category.clear()
        self.invalid_category_reasons.clear()
        self.missing_category_or_subcategory = 0
        self.rss_issues_total = 0
        self.rss_issues_by_type.clear()
        self.rss_issues_by_domain.clear()

    # ---------------- Scan logs (fallback) ------------
    def scan_logs_for_date(self, logs_dir: str, dt: date) -> None:
        """Parse log files in logs_dir and update counters for a given date.
        This is a best-effort parser relying on plain-text patterns produced by the app.
        """
        date_prefix = dt.strftime("%Y-%m-%d")
        try:
            files = [os.path.join(logs_dir, f) for f in os.listdir(logs_dir) if f.endswith('.log')]
        except Exception as e:
            logger.warning(f"StatsCollector: cannot list logs in {logs_dir}: {e}")
            return

        import re
        sub_re = re.compile(r"\[LM SUB\].*?'([^']+)'.*?'([^']+)'", re.IGNORECASE)
        invalid_re = re.compile(r"\[LM INVALID\].*?:\s*(.+)$", re.IGNORECASE)
        missing_re = re.compile(r"пустая\s+category\s+или\s+subcategory", re.IGNORECASE)

        # RSS patterns
        http_re = re.compile(r"HTTP[^\d]*(\d{3})", re.IGNORECASE)
        connect_re = re.compile(r"Ошибка подключения|connection error", re.IGNORECASE)
        timeout_re = re.compile(r"таймаут|timeout", re.IGNORECASE)
        parse_re = re.compile(r"ошибка парсинга|parse error", re.IGNORECASE)
        fetchfail_re = re.compile(r"Не удалось получить|failed to fetch", re.IGNORECASE)

        for path in files:
            try:
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        if date_prefix not in line:
                            continue
                        # LM SUB
                        m = sub_re.search(line)
                        if m:
                            sub, cat = m.group(1), m.group(2)
                            self.record_sub_mismatch(cat, sub)
                            continue
                        # LM INVALID
                        m2 = invalid_re.search(line)
                        if m2:
                            self.record_invalid_category(m2.group(1).strip())
                            continue
                        # Missing category/subcategory
                        if missing_re.search(line):
                            self.record_missing_category()
                            continue
                        # RSS issues
                        mhttp = http_re.search(line)
                        if mhttp:
                            code = mhttp.group(1)
                            self.record_rss_issue("", "", f"http_status_{code}")
                            continue
                        if connect_re.search(line):
                            self.record_rss_issue("", "", "connection_error")
                            continue
                        if timeout_re.search(line):
                            self.record_rss_issue("", "", "timeout")
                            continue
                        if parse_re.search(line):
                            self.record_rss_issue("", "", "parse_error")
                            continue
                        if fetchfail_re.search(line):
                            self.record_rss_issue("", "", "fetch_failed")
                            continue
            except Exception as e:
                logger.warning(f"StatsCollector: cannot parse log {path}: {e}")
