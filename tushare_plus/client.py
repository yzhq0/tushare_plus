"""Tushare API 客户端

本模块提供了访问 Tushare 金融数据 API 的客户端实现。
主要功能：
1. 自动探测并记录各接口的单次传输限制和访问频率限制
2. 自动处理分页请求，支持获取超过单次传输限制的数据
3. 支持并发请求，提高大量数据获取效率
4. 实现访问频率控制，避免触发 API 调用限制
5. 错误处理和自动重试机制

使用示例：
    client = TushareAPI(token="your_token_here")
    
    # 获取股票基本信息
    df = client.get_data(
        api_name="stock_basic",
        fields="ts_code,name,industry,area",
        list_status="L"
    )
    
    # 获取大量日线数据（自动处理分页）
    df_daily = client.get_data(
        api_name="daily",
        fields="ts_code,trade_date,open,high,low,close,vol",
        limit=240000
    )
"""

import json
import time
import logging
import os
import csv
import copy
import datetime
import hashlib
import math
import random
import re
import threading
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from urllib.request import ProxyHandler, Request, build_opener
import pandas as pd
import concurrent.futures
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TushareAPI')

# DEFAULT_API_LIMITS_FILENAME = "api_limits.csv" # 不再需要全局默认，由各API类指定
CONFIG_DIR_NAME = ".tushare_plus"


class APIResponseError(Exception):
    """API returned a non-zero code."""

    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(f"Error {code}: {message}")


class PaginationError(Exception):
    """Base class for pagination failures.

    ``report`` contains the progress observed before the failure whenever it is
    available.  Keeping it on the exception makes strict, fail-closed paging
    auditable without changing the successful ``get_data`` return value.
    """

    def __init__(self, message, report=None):
        self.report = report
        super().__init__(message)


class PaginationProtocolError(PaginationError):
    """The server returned internally inconsistent pagination metadata."""


class PaginationIncompleteError(PaginationError):
    """A client-side safety bound was reached before exhaustion was proven."""


class DuplicateKeyError(PaginationProtocolError):
    """Rows from paged responses contain duplicate caller-declared keys."""


class PaginationReport:
    """Structured description of one ``get_data`` pagination execution.

    This deliberately avoids ``dataclasses`` so the package remains importable
    on Python 3.6.
    """

    def __init__(
        self,
        api_name,
        mode,
        page_size=None,
        pages_requested=0,
        pages_completed=0,
        rows_fetched=0,
        termination_reason=None,
        complete=False,
        last_has_more=None,
        start_offset=0,
        user_limit=None,
        max_pages=None,
        duplicate_key_count=0,
        pages=None,
        request_satisfied=False,
        source_exhausted=False,
        exhaustion_inferred=False,
    ):
        self.api_name = api_name
        self.mode = mode
        self.page_size = page_size
        self.pages_requested = pages_requested
        self.pages_completed = pages_completed
        self.rows_fetched = rows_fetched
        self.termination_reason = termination_reason
        self.complete = complete
        self.last_has_more = last_has_more
        self.start_offset = start_offset
        self.user_limit = user_limit
        self.max_pages = max_pages
        self.duplicate_key_count = duplicate_key_count
        self.pages = list(pages or [])
        self.request_satisfied = request_satisfied
        self.source_exhausted = source_exhausted
        self.exhaustion_inferred = exhaustion_inferred

    def to_dict(self):
        return {
            "api_name": self.api_name,
            "mode": self.mode,
            "page_size": self.page_size,
            "pages_requested": self.pages_requested,
            "pages_completed": self.pages_completed,
            "rows_fetched": self.rows_fetched,
            "termination_reason": self.termination_reason,
            "complete": self.complete,
            "last_has_more": self.last_has_more,
            "start_offset": self.start_offset,
            "user_limit": self.user_limit,
            "max_pages": self.max_pages,
            "duplicate_key_count": self.duplicate_key_count,
            "pages": [dict(page) for page in self.pages],
            "request_satisfied": self.request_satisfied,
            "source_exhausted": self.source_exhausted,
            "exhaustion_inferred": self.exhaustion_inferred,
        }


class PartitionPlan:
    """Immutable description of a partitioned download.

    ``param_chunks`` is materialized immediately so that planning, collision
    detection and the execution manifest all refer to the same ordered work
    set.  The class is intentionally implemented without dataclasses for
    Python 3.6 compatibility.
    """

    def __init__(
        self,
        api_name,
        param_chunks,
        output_dir,
        fields="",
        file_format="csv",
        base_params=None,
        partition_filename=None,
        auto_paging=True,
        concurrent=False,
        max_pages=None,
        limit_per_request=None,
        detect_limit=True,
        primary_key=None,
        strict_paging=True,
        partition_workers=1,
    ):
        object.__setattr__(self, "_frozen", False)
        if not isinstance(api_name, str) or not api_name.strip():
            raise ValueError("api_name must be a non-empty string")
        chunks = list(param_chunks)
        if any(not isinstance(chunk, dict) for chunk in chunks):
            raise ValueError("every param chunk must be a mapping")
        if not chunks:
            raise ValueError("param_chunks must not be empty")
        normalized_format = str(file_format).lower()
        if normalized_format not in {"csv", "parquet"}:
            raise ValueError("file_format must be 'csv' or 'parquet'")
        if partition_filename is not None and not callable(partition_filename):
            raise ValueError("partition_filename must be callable")
        if (
            isinstance(partition_workers, bool)
            or not isinstance(partition_workers, int)
            or partition_workers <= 0
        ):
            raise ValueError("partition_workers must be a positive integer")
        if max_pages is not None and (
            isinstance(max_pages, bool)
            or not isinstance(max_pages, int)
            or max_pages <= 0
        ):
            raise ValueError("max_pages must be a positive integer")
        if limit_per_request is not None and (
            isinstance(limit_per_request, bool)
            or not isinstance(limit_per_request, int)
            or limit_per_request <= 0
        ):
            raise ValueError("PartitionPlan limit_per_request must be positive")
        if base_params is not None and not isinstance(base_params, dict):
            raise ValueError("base_params must be a mapping")
        for option_name, option_value in (
            ("auto_paging", auto_paging),
            ("concurrent", concurrent),
            ("detect_limit", detect_limit),
            ("strict_paging", strict_paging),
        ):
            if not isinstance(option_value, bool):
                raise ValueError("{0} must be boolean".format(option_name))

        if not isinstance(fields, str):
            raise ValueError("fields must be a string")

        object.__setattr__(self, "api_name", api_name.strip())
        object.__setattr__(self, "_param_chunks", copy.deepcopy(chunks))
        object.__setattr__(self, "output_dir", Path(output_dir))
        object.__setattr__(self, "fields", fields)
        object.__setattr__(self, "file_format", normalized_format)
        object.__setattr__(self, "_base_params", copy.deepcopy(base_params or {}))
        object.__setattr__(self, "partition_filename", partition_filename)
        object.__setattr__(self, "auto_paging", auto_paging)
        object.__setattr__(self, "concurrent", concurrent)
        object.__setattr__(self, "max_pages", max_pages)
        object.__setattr__(self, "limit_per_request", limit_per_request)
        object.__setattr__(self, "detect_limit", detect_limit)
        object.__setattr__(self, "_primary_key", copy.deepcopy(primary_key))
        object.__setattr__(self, "strict_paging", strict_paging)
        object.__setattr__(self, "partition_workers", partition_workers)
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name, value):
        if getattr(self, "_frozen", False):
            raise AttributeError("PartitionPlan is immutable")
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if getattr(self, "_frozen", False):
            raise AttributeError("PartitionPlan is immutable")
        object.__delattr__(self, name)

    @property
    def param_chunks(self):
        return copy.deepcopy(self._param_chunks)

    @property
    def base_params(self):
        return copy.deepcopy(self._base_params)

    @property
    def primary_key(self):
        return copy.deepcopy(self._primary_key)


class PartitionResult:
    """Outcome of one partition request."""

    def __init__(
        self,
        index,
        status,
        params,
        path,
        sidecar_path,
        query_fingerprint,
        row_count=None,
        sha256=None,
        pagination_report=None,
        error_type=None,
        error_message=None,
        checkpoint_status=None,
        artifact_secrets=None,
    ):
        self.index = index
        self.status = status
        self.params = params
        self.path = str(path)
        self.sidecar_path = str(sidecar_path)
        self.query_fingerprint = query_fingerprint
        self.row_count = row_count
        self.sha256 = sha256
        self.pagination_report = pagination_report
        self.error_type = error_type
        self.error_message = error_message
        self.checkpoint_status = checkpoint_status
        self._artifact_secrets = list(artifact_secrets or [])

    def to_dict(self):
        payload = {
            "index": self.index,
            "status": self.status,
            "params": _scrub_secret_values(
                _redact_sensitive(self.params), self._artifact_secrets
            ),
            "path": _scrub_secret_values(self.path, self._artifact_secrets),
            "sidecar_path": _scrub_secret_values(
                self.sidecar_path, self._artifact_secrets
            ),
            "query_fingerprint": self.query_fingerprint,
            "row_count": self.row_count,
            "sha256": self.sha256,
            "pagination_report": (
                _scrub_secret_values(
                    dict(self.pagination_report), self._artifact_secrets
                )
                if self.pagination_report is not None
                else None
            ),
            "error_type": self.error_type,
            "error_message": _scrub_secret_values(
                self.error_message, self._artifact_secrets
            ),
            "checkpoint_status": self.checkpoint_status,
        }
        return payload


class PartitionExecutionResult:
    """Structured result also serialized as the execution manifest."""

    def __init__(
        self,
        api_name,
        plan_fingerprint,
        started_at,
        finished_at,
        partitions,
        manifest_path,
    ):
        self.schema_version = 1
        self.api_name = api_name
        self.plan_fingerprint = plan_fingerprint
        self.started_at = started_at
        self.finished_at = finished_at
        self.partitions = list(partitions)
        self.manifest_path = str(manifest_path)
        self._artifact_secrets = []
        for item in self.partitions:
            self._artifact_secrets.extend(item._artifact_secrets)
        self.total_partitions = len(self.partitions)
        self.written = sum(item.status == "written" for item in self.partitions)
        self.resumed = sum(item.status == "resumed" for item in self.partitions)
        self.failed = sum(item.status == "failed" for item in self.partitions)
        self.not_run = sum(item.status == "not_run" for item in self.partitions)
        self.complete = self.failed == 0 and all(
            item.status in {"written", "resumed"} for item in self.partitions
        )

    @property
    def paths(self):
        return [
            Path(item.path)
            for item in self.partitions
            if item.status in {"written", "resumed"}
        ]

    def to_dict(self):
        return {
            "schema_version": self.schema_version,
            "api_name": _scrub_secret_values(
                self.api_name, self._artifact_secrets
            ),
            "plan_fingerprint": self.plan_fingerprint,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "complete": self.complete,
            "total_partitions": self.total_partitions,
            "written": self.written,
            "resumed": self.resumed,
            "failed": self.failed,
            "not_run": self.not_run,
            "manifest_path": _scrub_secret_values(
                self.manifest_path, self._artifact_secrets
            ),
            "partitions": [item.to_dict() for item in self.partitions],
        }


class PartitionExecutionError(Exception):
    """One or more partitions failed; ``result`` points to the manifest data."""

    def __init__(self, result):
        self.result = result
        super().__init__(
            "partition execution failed for {0} of {1} partitions; manifest={2}".format(
                result.failed,
                result.total_partitions,
                result.manifest_path,
            )
        )


class PartitionCheckpointError(Exception):
    """Existing partition artifacts cannot be safely associated with a plan."""


class PartitionLockError(Exception):
    """Another execution owns an overlapping output or manifest lock."""


_SENSITIVE_KEYS = {
    "token",
    "access_token",
    "password",
    "passwd",
    "secret",
    "client_secret",
    "api_key",
    "apikey",
    "authorization",
}


def _is_sensitive_key(key):
    normalized = str(key).strip().lower()
    return normalized in _SENSITIVE_KEYS or normalized.endswith(
        ("_token", "_password", "_secret", "_api_key")
    )


def _json_safe(value):
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical parameter mappings require string keys")
        return {key: _json_safe(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise TypeError("canonical parameters cannot contain NaN or infinity")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(
        "value is not canonically JSON serializable: {0}".format(
            type(value).__name__
        )
    )


def _redact_sensitive(value):
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical parameter mappings require string keys")
        result = {}
        for key, item in value.items():
            if _is_sensitive_key(key):
                result[str(key)] = "***REDACTED***"
            else:
                result[str(key)] = _redact_sensitive(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_redact_sensitive(item) for item in value]
    return _json_safe(value)


def _sensitive_values(value):
    found = []
    if isinstance(value, dict):
        for key, item in value.items():
            if _is_sensitive_key(key):
                if item is not None and str(item):
                    found.append(str(item))
            else:
                found.extend(_sensitive_values(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_sensitive_values(item))
    return found


def _redact_error_message(error, params, extra_secrets=None):
    message = str(error)
    secrets = _sensitive_values(params)
    for item in extra_secrets or []:
        if item is not None and str(item):
            secrets.append(str(item))
    for secret in sorted(set(secrets), key=len, reverse=True):
        message = message.replace(secret, "***REDACTED***")
    return message


def _scrub_secret_values(value, secrets):
    """Recursively remove known secret values from persisted audit data."""
    normalized = sorted(
        {str(secret) for secret in secrets if secret is not None and str(secret)},
        key=len,
        reverse=True,
    )
    if isinstance(value, dict):
        return {
            key: _scrub_secret_values(item, normalized)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_scrub_secret_values(item, normalized) for item in value]
    if isinstance(value, tuple):
        return [_scrub_secret_values(item, normalized) for item in value]
    if isinstance(value, str):
        scrubbed = value
        for secret in normalized:
            if scrubbed == secret:
                scrubbed = "***REDACTED***"
            elif len(secret) >= 8:
                scrubbed = scrubbed.replace(secret, "***REDACTED***")
        return scrubbed
    return value


def _canonical_fingerprint(payload):
    serialized = json.dumps(
        _redact_sensitive(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(str(path), "rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _utc_now_text():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _temporary_sibling(path):
    return path.parent / (".{0}.{1}.tmp".format(path.name, uuid.uuid4().hex))


def _fsync_file(path):
    with open(str(path), "rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path):
    """Best-effort persistence for directory entries after ``os.replace``.

    Some supported platforms do not permit opening or fsyncing directories.
    File fsync remains strict; an unsupported directory fsync must not turn a
    successfully committed artifact into an application-level failure.
    """
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            return
    finally:
        os.close(descriptor)


def _atomic_write_json(path, payload):
    temporary = _temporary_sibling(path)
    try:
        with open(str(temporary), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temporary), str(path))
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _acquire_execution_locks(paths, plan_fingerprint):
    owner = uuid.uuid4().hex
    acquired = []
    normalized_paths = sorted(
        {os.path.abspath(str(path)) for path in paths},
        key=os.path.normcase,
    )
    try:
        for text_path in normalized_paths:
            path = Path(text_path)
            created_stat = None
            try:
                descriptor = os.open(
                    str(path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
                created_stat = os.fstat(descriptor)
            except FileExistsError:
                raise PartitionLockError(
                    "partition execution lock already exists: {0}; if no process "
                    "owns it, inspect and remove the stale lock explicitly".format(path)
                )
            try:
                payload = json.dumps(
                    {
                        "schema_version": 1,
                        "owner": owner,
                        "pid": os.getpid(),
                        "created_at": _utc_now_text(),
                        "plan_fingerprint": plan_fingerprint,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    descriptor = None
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
            except Exception:
                # This path was created by this call but is not yet in
                # ``acquired``.  Remove it explicitly on a payload/fsync
                # failure so a failed acquisition cannot strand a stale lock.
                if descriptor is not None:
                    os.close(descriptor)
                    descriptor = None
                try:
                    current_stat = os.stat(str(path), follow_symlinks=False)
                    if (
                        created_stat is not None
                        and current_stat.st_dev == created_stat.st_dev
                        and current_stat.st_ino == created_stat.st_ino
                    ):
                        path.unlink()
                except OSError:
                    pass
                raise
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            acquired.append(path)
        return owner, acquired
    except Exception:
        _release_execution_locks(owner, acquired)
        raise


def _release_execution_locks(owner, paths):
    for path in reversed(list(paths)):
        try:
            with open(str(path), "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict) and payload.get("owner") == owner:
                path.unlink()
        except (OSError, ValueError):
            # A changed/unreadable lock is not silently removed because it may
            # have been replaced by another owner.
            continue


def _safe_filename_part(value) -> str:
    text = str(value)
    text = re.sub(r"[^0-9A-Za-z._=-]+", "_", text)
    return text.strip("._") or "empty"

class APILimitDetector:
    def __init__(self, csv_path: Optional[str] = None, default_filename: str = "api_limits.csv"):
        """初始化API限制参数检测器
        
        参数:
            csv_path: API限制参数CSV文件的路径。如果为None，则使用用户目录下的默认路径。
            default_filename: 当 csv_path 为 None 时，在用户目录下使用的默认文件名。
        """
        if csv_path is None:
            user_home = os.path.expanduser("~")
            config_dir = os.path.join(user_home, CONFIG_DIR_NAME)
            self.csv_path = os.path.join(config_dir, default_filename) # 使用传入的 default_filename
            os.makedirs(config_dir, exist_ok=True)
            logger.info(f"API限制参数文件将使用默认路径: {self.csv_path}")
        else:
            self.csv_path = csv_path
            dir_name = os.path.dirname(self.csv_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            logger.info(f"API限制参数文件将使用指定路径: {self.csv_path}")

        self._init_csv()
    
    def _init_csv(self):
        """初始化CSV文件"""
        if not os.path.exists(self.csv_path):
            # 创建CSV文件并写入表头
            with open(self.csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['api_name', 'limit_per_request', 'rate_limit', 'last_updated'])
    
    def get_api_limits(self, api_name: str) -> Optional[Dict]:
        """从CSV文件获取API限制参数"""
        if not os.path.exists(self.csv_path):
            return None
            
        try:
            df = pd.read_csv(self.csv_path)
            row = df[df['api_name'] == api_name]
            if not row.empty:
                # 确保返回的是Python原生类型，而不是NumPy类型
                return {
                    "limit_per_request": int(row['limit_per_request'].values[0]),
                    "rate_limit": int(row['rate_limit'].values[0]),
                    "last_updated": row['last_updated'].values[0]
                }
        except Exception as e:
            logger.warning(f"读取API限制参数失败: {str(e)}")
        
        return None
    
    def save_api_limits(self, api_name: str, limit_per_request: int, rate_limit: int):
        """保存API限制参数到CSV文件"""
        try:
            # 读取现有数据
            if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
                df = pd.read_csv(self.csv_path)
                # 更新或添加记录
                if api_name in df['api_name'].values:
                    df.loc[df['api_name'] == api_name, 'limit_per_request'] = limit_per_request
                    df.loc[df['api_name'] == api_name, 'rate_limit'] = rate_limit
                    df.loc[df['api_name'] == api_name, 'last_updated'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    new_row = pd.DataFrame({
                        'api_name': [api_name],
                        'limit_per_request': [limit_per_request],
                        'rate_limit': [rate_limit],
                        'last_updated': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                    })
                    df = pd.concat([df, new_row], ignore_index=True)
            else:
                # 创建新的DataFrame
                df = pd.DataFrame({
                    'api_name': [api_name],
                    'limit_per_request': [limit_per_request],
                    'rate_limit': [rate_limit],
                    'last_updated': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                })
            
            # 保存到CSV
            df.to_csv(self.csv_path, index=False)
            logger.info(f"API限制参数已保存到 {self.csv_path}")
        except Exception as e:
            logger.error(f"保存API限制参数失败: {str(e)}")

    def remove_api_limits(self, api_name: str):
        """从CSV文件删除指定API的限制参数"""
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            logger.warning(f"API限制文件 {self.csv_path} 不存在或为空，无法删除 {api_name} 的限制。")
            return

        try:
            df = pd.read_csv(self.csv_path)
            if api_name in df['api_name'].values:
                df_filtered = df[df['api_name'] != api_name]
                # 如果过滤后DataFrame为空，to_csv会写入一个只有表头的文件
                df_filtered.to_csv(self.csv_path, index=False)
                logger.info(f"已从 {self.csv_path} 删除 {api_name} 的限制参数。")
            else:
                logger.info(f"在 {self.csv_path} 中未找到 {api_name} 的限制参数，无需删除。")
        except Exception as e:
            logger.error(f"从 {self.csv_path} 删除 {api_name} 的限制参数失败: {str(e)}")

class TushareAPI:

    def __init__(
        self,
        token=None,
        max_workers=5,
        max_retries=3,
        retry_delay=1,
        retry_backoff=2.0,
        retry_jitter=0.1,
        max_retry_delay=60,
        request_timeout: Optional[float] = 60,
        enable_rate_limit=True,
        use_env_proxy: bool = True,
        custom_params_file=None,
        api_limits_file: Optional[str] = None,
        api_limits_default_filename: str = "tushare_api_limits.csv" # 新增参数，TushareAPI的默认文件名
    ):
        # 创建实例级别的logger，使用实际的类名
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if token:
            self.token = token
        else:
            self.token = os.environ.get('TUSHARE_TOKEN')

        if not self.token:
            raise ValueError("Tushare token must be provided either as an argument or via TUSHARE_TOKEN environment variable.")

        self.api_url = "http://api.tushare.pro"
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retry_jitter = retry_jitter
        self.max_retry_delay = max_retry_delay
        self.request_timeout = request_timeout
        self.use_env_proxy = use_env_proxy
        self._url_opener = self._build_url_opener()
        # APILimitDetector 会根据 api_limits_file 是否为 None 来决定路径
        # 如果 api_limits_file 为 None，则使用 api_limits_default_filename 在用户目录下创建文件
        self.limit_detector = APILimitDetector(
            csv_path=api_limits_file, 
            default_filename=api_limits_default_filename
        )
        self._api_last_call_time = {}
        self._api_info_cache = {}  # 添加缓存初始化
        self._rate_limit_lock = threading.Lock()
        self.enable_rate_limit = enable_rate_limit  # 添加频率限制开关

        # 加载API参数配置
        self._api_required_params = self._load_api_params(custom_params_file)

    def _build_url_opener(self):
        if self.use_env_proxy:
            return build_opener()
        return build_opener(ProxyHandler({}))

    def _urlopen(self, request: Request, timeout: Optional[float] = None):
        effective_timeout = self.request_timeout if timeout is None else timeout
        if effective_timeout is None:
            return self._url_opener.open(request)
        return self._url_opener.open(request, timeout=effective_timeout)

    def _load_api_params(self, custom_params_file=None):
        """加载API参数配置
        
        参数:
            custom_params_file: 自定义参数配置文件路径，如果为None则使用默认配置
        
        返回:
            API参数配置字典
        """
        # 默认参数配置
        default_params = {
            "index_weight": {"index_code": "000906.SH"}  # 基本配置，作为备用
        }

        # 尝试加载默认配置文件
        default_params_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'api_params.json')
        if os.path.exists(default_params_file):
            try:
                with open(default_params_file, 'r', encoding='utf-8') as f:
                    default_params = json.load(f)
                    self.logger.info(f"已加载默认API参数配置: {default_params_file}")
            except Exception as e:
                self.logger.warning(f"加载默认API参数配置失败: {str(e)}")

        # 如果提供了自定义配置文件，合并配置
        if custom_params_file and os.path.exists(custom_params_file):
            try:
                with open(custom_params_file, 'r', encoding='utf-8') as f:
                    custom_params = json.load(f)
                    # 合并配置，自定义配置优先
                    default_params.update(custom_params)
                    self.logger.info(f"已加载自定义API参数配置: {custom_params_file}")
            except Exception as e:
                self.logger.warning(f"加载自定义API参数配置失败: {str(e)}")

        return default_params

    def add_api_params(self, api_name, params):
        """添加或更新API参数
        
        参数:
            api_name: API接口名称
            params: 参数字典
        """
        if not isinstance(params, dict):
            raise ValueError("params must be a mapping")
        self._api_required_params[api_name] = copy.deepcopy(params)
        self.logger.info(
            f"已添加API参数: {api_name} = {_redact_sensitive(params)}"
        )

    def _detect_api_limits(self, api_name: str) -> Tuple[int, int]:
        """探测API的限制参数
        
        参数:
            api_name: API接口名称
        """
        self.logger.info(f"开始探测接口 {api_name} 的限制参数...")

        # 使用预定义的必要参数，不合并用户传入的参数
        required_params = self._api_required_params.get(api_name, {}).copy()

        # 首先探测单次请求限制
        limit = self._detect_request_limit(api_name, required_params)

        # 然后探测访问频率限制
        rate_limit = self._detect_rate_limit(api_name, required_params)

        # 保存探测结果
        self.limit_detector.save_api_limits(api_name, limit, rate_limit)
        self.logger.info(f"接口 {api_name} 的限制参数探测完成：单次限制 {limit}，频率限制 {rate_limit}/分钟")

        return limit, rate_limit

    def _detect_request_limit(
        self,
        api_name: str,
        required_params: Dict = None,
        fields: str = "",
    ) -> int:
        """探测单次请求数据量限制
        
        参数:
            api_name: API接口名称
            required_params: 必要的请求参数
        """
        if required_params is None:
            required_params = {}

        try:
            self.logger.info(f"开始探测接口 {api_name} 的单次请求限制...")
            # 构造请求参数，包含必要参数
            params = required_params.copy()

            # 不设置limit参数，直接请求
            payload = {
                "api_name": api_name,
                "token": self.token,
                "params": params,
                "fields": fields
            }
            req = Request(
                self.api_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with self._urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                if result["code"] != 0:
                    safe_message = _redact_error_message(
                        result["msg"],
                        params,
                        extra_secrets=[self.token],
                    )
                    raise Exception(f"Error {result['code']}: {safe_message}")
                count, has_more = self._validate_limit_probe_data(result.get("data"))

                if has_more is not None:
                    # 如果API返回了has_more字段
                    if not has_more:
                        # has_more=False only proves that this particular probe
                        # query is exhausted.  It does not prove that a wider
                        # query to the endpoint is unlimited.  Use the observed
                        # row count as a conservative page size instead of
                        # caching the unsafe historical sentinel value 0.
                        conservative_limit = count if count > 0 else 1
                        self.logger.info(
                            f"接口 {api_name} 的探测查询已取尽，采用保守分页大小 "
                            f"{conservative_limit}"
                        )
                        return conservative_limit
                    else:
                        # has_more为True，说明有更多数据，当前返回量可能是单次限制
                        self.logger.info(f"接口 {api_name} 的单次请求限制为 {count} 条")
                        return count
                else:
                    # 如果API没有返回has_more字段，使用原来的判断逻辑
                    # Without has_more the endpoint cap is unknowable.  The
                    # observed positive row count is still a safe page size for
                    # the current query; zero rows fall back to the documented
                    # conservative default.
                    conservative_limit = count if count > 0 else 1
                    self.logger.info(
                        f"接口 {api_name} 未返回 has_more，采用保守分页大小 "
                        f"{conservative_limit}"
                    )
                    return conservative_limit
        except Exception as e:
            safe_error = _redact_error_message(
                e,
                required_params,
                extra_secrets=[self.token],
            )
            self.logger.warning(f"探测接口 {api_name} 的单次请求限制失败: {safe_error}")
            # 失败时尝试手动指定限制值进行重试
            self.logger.info(f"开始使用预设限制值重试探测接口 {api_name}...")
            
            # 定义尝试的限制值列表，从50万开始，按优化步长递减
            retry_limits = [500000, 200000, 100000, 50000, 20000, 10000, 5000]
            
            for limit_value in retry_limits:
                try:
                    self.logger.info(f"尝试使用限制值 {limit_value} 探测接口 {api_name}...")
                    params = required_params.copy()
                    params["limit"] = limit_value
                    
                    payload = {
                        "api_name": api_name,
                        "token": self.token,
                        "params": params,
                        "fields": fields
                    }
                    req = Request(
                        self.api_url,
                        data=json.dumps(payload).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST"
                    )
                    
                    # Reuse the client-level timeout contract; probing should
                    # not have a separate hard-coded blocking duration.
                    with self._urlopen(req) as response:
                        result = json.loads(response.read().decode("utf-8"))
                        if result["code"] != 0:
                            safe_message = _redact_error_message(
                                result["msg"],
                                params,
                                extra_secrets=[self.token],
                            )
                            self.logger.warning(f"限制值 {limit_value} 请求失败: {safe_message}")
                            continue
                        
                        count, unused_has_more = self._validate_limit_probe_data(
                            result.get("data")
                        )
                        
                        # Cache the observed effective page size, not the much
                        # larger requested probe value.  Returning limit_value
                        # here used to make concurrent offsets skip data when a
                        # server silently capped the response.
                        effective_limit = count if count > 0 else 1
                        self.logger.info(
                            f"成功使用限制值 {limit_value} 探测接口 {api_name}，"
                            f"实际返回 {count} 条，采用分页大小 {effective_limit}"
                        )
                        return effective_limit
                        
                except Exception as retry_error:
                    safe_error = _redact_error_message(
                        retry_error,
                        params,
                        extra_secrets=[self.token],
                    )
                    self.logger.warning(f"使用限制值 {limit_value} 探测失败: {safe_error}")
                    # 继续尝试下一个更小的限制值
                    time.sleep(0.5)  # 短暂延迟，避免频繁请求
                    continue
            
            # Unknown is not equivalent to 5000.  A guessed large stride can
            # skip rows in concurrent mode when the recovered server silently
            # applies a smaller cap.  One row is slow but fail-closed; callers
            # with a verified profile can explicitly pass limit_per_request.
            self.logger.warning(
                f"所有重试尝试均失败，接口 {api_name} 使用保守分页大小 1"
            )
            return 1

    @staticmethod
    def _validate_limit_probe_data(data):
        """Validate the small response contract used to infer a page stride."""
        if not isinstance(data, dict):
            raise PaginationProtocolError("limit probe data must be a mapping")
        items = data.get("items")
        if not isinstance(items, (list, tuple)):
            raise PaginationProtocolError("limit probe items must be an array")
        has_more = data.get("has_more", None)
        if has_more is not None and not isinstance(has_more, bool):
            raise PaginationProtocolError(
                "limit probe has_more must be boolean or null"
            )
        if not items and has_more is True:
            raise PaginationProtocolError(
                "limit probe returned an empty page with has_more=True"
            )
        return len(items), has_more

    def _detect_rate_limit(self, api_name: str, required_params: Dict = None) -> int:
        """探测每分钟访问频率限制
        
        参数:
            api_name: API接口名称
            required_params: 必要的请求参数
        """
        if required_params is None:
            required_params = {}

        # 使用小数据量快速测试
        test_limit = 100
        count = 0
        start_time = time.time()
        
        # 初始化该API的访问历史记录（用于后续频率控制）
        if not hasattr(self, '_api_call_history'):
            self._api_call_history = {}
        if api_name not in self._api_call_history:
            self._api_call_history[api_name] = []

        # 构造请求参数，包含必要参数
        params = required_params.copy()
        params["limit"] = test_limit

        # 避免循环调用，直接发送请求
        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": ""
        }

        while time.time() - start_time < 60:
            try:
                req = Request(
                    self.api_url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with self._urlopen(req) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    if result["code"] != 0:
                        if "每分钟最多访问" in result["msg"]:
                            break
                        safe_message = _redact_error_message(
                            result["msg"],
                            params,
                            extra_secrets=[self.token],
                        )
                        raise Exception(f"Error {result['code']}: {safe_message}")
                count += 1
                # 记录本次调用时间到访问历史中
                self._api_call_history[api_name].append(time.time())
                # 短暂休息以避免立即触发限制
                # time.sleep(0.1)
            except Exception as e:
                if "每分钟最多访问" in str(e):
                    break
                else:
                    raise e


        detected_limit = max(1, count)
        
        # 如果探测过程中达到了限制，记录最后一次请求的时间
        if count > 0 and len(self._api_call_history[api_name]) > 0:
            self.logger.info(f"接口 {api_name} 在探测过程中发送了 {count} 次请求，可能需要等待API限制重置")
            
        return detected_limit

    def get_api_info(
        self,
        api_name: str,
        probe_params: Optional[Dict] = None,
        fields: str = "",
    ) -> Dict:
        """获取API接口信息，如果没有则进行探测
        
        参数:
            api_name: API接口名称
        """
        # 尝试从缓存获取
        if api_name in self._api_info_cache:
            cached_memory = self._api_info_cache[api_name]
            if int(cached_memory.get("limit_per_request", 0)) > 0:
                return cached_memory
            # Version 0.1.8 used 0 to mean "unlimited" after a narrow
            # has_more=False probe.  Treat that legacy value as unknown.
            del self._api_info_cache[api_name]

        # 如果禁用了频率限制，使用0表示无限制
        if not self.enable_rate_limit:
            # 只探测单次请求限制，不探测频率限制
            cached_limits = self.limit_detector.get_api_limits(api_name)
            if cached_limits is None or int(cached_limits["limit_per_request"]) <= 0:
                # 没有缓存，只探测单次请求限制
                detection_params = (
                    probe_params.copy()
                    if probe_params is not None
                    else self._api_required_params.get(api_name, {}).copy()
                )
                limit_per_request = self._detect_request_limit(
                    api_name,
                    detection_params,
                    fields=fields,
                )
                rate_limit = 0  # 使用0表示没有频率限制
                # 保存探测结果到CSV文件
                self.limit_detector.save_api_limits(api_name, limit_per_request, rate_limit)
            else:
                limit_per_request = int(cached_limits["limit_per_request"])
                rate_limit = 0  # 使用0表示没有频率限制
        else:
            # 尝试从CSV文件获取缓存的限制参数
            cached_limits = self.limit_detector.get_api_limits(api_name)

            if cached_limits is None or int(cached_limits["limit_per_request"]) <= 0:
                # 没有缓存，进行探测
                detection_params = (
                    probe_params.copy()
                    if probe_params is not None
                    else self._api_required_params.get(api_name, {}).copy()
                )
                limit_per_request = self._detect_request_limit(
                    api_name,
                    detection_params,
                    fields=fields,
                )
                rate_limit = self._detect_rate_limit(api_name, detection_params)
                self.limit_detector.save_api_limits(
                    api_name,
                    limit_per_request,
                    rate_limit,
                )
                
                # 探测完成后，检查是否需要等待API限制重置
                # 确保在探测后有足够的时间间隔再进行实际数据请求
                if hasattr(self, '_api_call_history') and api_name in self._api_call_history and len(self._api_call_history[api_name]) > 0:
                    self._respect_rate_limit(api_name)
            else:
                # 确保是Python原生类型
                limit_per_request = int(cached_limits["limit_per_request"])
                rate_limit = int(cached_limits["rate_limit"])

        # 保存到缓存
        info = {
            "limit_per_request": limit_per_request,
            "rate_limit": rate_limit
        }
        self._api_info_cache[api_name] = info
        return info

    def clear_api_limits(self, api_name: str):
        """清除指定API的限制参数（内存缓存和CSV文件）"""
        self.logger.info(f"开始清除接口 {api_name} 的限制参数...")

        # 从CSV文件清除
        self.limit_detector.remove_api_limits(api_name)

        # 从内存缓存清除
        if api_name in self._api_info_cache:
            del self._api_info_cache[api_name]
            self.logger.info(f"已从缓存中清除接口 {api_name} 的限制参数。")
        else:
            self.logger.info(f"接口 {api_name} 的限制参数未在内存缓存中找到。")

    def force_redetect_api_limits(self, api_name: str):
        """
        强制清除并重新探测指定API的限制参数。
        此方法会首先清除该API在内存缓存和CSV文件中的现有记录，
        然后立即触发新的限制参数探测过程。
        """

        # 1. 清除现有的限制参数 (包括内存缓存和CSV文件中的记录)
        # clear_api_limits 方法内部会处理详细的日志记录
        self.clear_api_limits(api_name)

        # 2. 重新获取API信息，这将触发探测逻辑（因为缓存已被清除）
        # get_api_info 方法会负责探测、保存到CSV并更新内存缓存
        self.logger.info(f"缓存清除完毕，开始为接口 {api_name} 重新进行参数探测。")
        try:
            # 调用 get_api_info 会触发探测（如果需要）并返回更新后的信息
            new_limits = self.get_api_info(api_name)

        except Exception as e:
            safe_error = _redact_error_message(
                e,
                self._api_required_params.get(api_name, {}),
                extra_secrets=[self.token],
            )
            self.logger.error(
                f"在为接口 {api_name} 强制重新探测参数时发生错误: {safe_error}"
            )
            # 即使探测失败，之前的清除操作也已完成
            self.logger.info(f"接口 {api_name} 的旧有参数已被清除，但新的探测未能成功。请检查错误信息。")

    def _retry_sleep(self, retry_count: int) -> None:
        delay = self.retry_delay * (self.retry_backoff ** retry_count)
        if self.max_retry_delay is not None:
            delay = min(delay, self.max_retry_delay)
        if self.retry_jitter:
            delay += random.uniform(0, delay * self.retry_jitter)
        if delay > 0:
            time.sleep(delay)

    def _make_request(self, api_name, params, fields, retry_count=0):
        """构造并发送HTTP POST请求，支持重试机制"""
        # 检查并遵守访问频率限制
        # 避免循环调用，只在非探测模式下检查频率限制
        if self.enable_rate_limit and api_name in self._api_info_cache:
            self._respect_rate_limit(api_name)

        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": fields
        }
        req = Request(
            self.api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with self._urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                if result["code"] != 0:
                    safe_message = _redact_error_message(
                        result["msg"],
                        params,
                        extra_secrets=[self.token],
                    )
                    # 记录错误并根据错误类型定义是否重试
                    if retry_count < self.max_retries and self._should_retry(result["code"]):
                        error_msg = f"Error {result['code']}: {safe_message}"
                        self.logger.warning(f"{api_name} 请求失败，将重试: {error_msg}")
                        self._retry_sleep(retry_count)
                        return self._make_request(api_name, params, fields, retry_count + 1)
                    raise APIResponseError(result["code"], safe_message)
                return result["data"]
        except Exception as e:
            if isinstance(e, APIResponseError) and not self._should_retry(e.code):
                raise
            if retry_count < self.max_retries:
                safe_error = _redact_error_message(
                    e,
                    params,
                    extra_secrets=[self.token],
                )
                self.logger.warning(f"{api_name} 请求失败，将重试: {safe_error}")
                self._retry_sleep(retry_count)
                return self._make_request(api_name, params, fields, retry_count + 1)
            safe_error = _redact_error_message(
                e,
                params,
                extra_secrets=[self.token],
            )
            raise Exception(
                f"Request failed after {self.max_retries} retries: {safe_error}"
            )

    def _should_retry(self, error_code):
        """根据错误码判断是否应该重试"""
        # 可以根据 API 文档中的错误码定义来完善此函数
        # 通常，网络错误、服务器临时错误应该重试，参数错误等不应重试
        retry_error_codes = [
            -1,  # 系统错误
            40203,  # 请求过于频繁
            500,  # 服务器内部错误
            503   # 服务不可用
        ]
        return error_code in retry_error_codes

    def _respect_rate_limit(self, api_name):
        # Partition-level concurrency can call this method from several worker
        # threads.  The check-and-append sequence must be atomic or every
        # worker can observe the same free slot.
        with self._rate_limit_lock:
            return self._respect_rate_limit_unlocked(api_name)

    def _respect_rate_limit_unlocked(self, api_name):
        """遵守 API 访问频率限制
        
        使用滑动窗口方式实现频率控制，确保在任意 60 秒内的请求次数不超过限制
        """
        # 获取接口的访问频率限制
        api_info = self._api_info_cache.get(api_name, {"rate_limit": 60})
        rate_limit = api_info.get('rate_limit', 60)  # 默认每分钟 60 次

        # 如果rate_limit为0，表示没有频率限制，直接返回
        if rate_limit == 0:
            return

        # 初始化该 API 的访问历史记录
        if not hasattr(self, '_api_call_history'):
            self._api_call_history = {}

        if api_name not in self._api_call_history:
            self._api_call_history[api_name] = []

        # 获取当前时间
        now = time.time()

        # 清理超过 60 秒的历史记录
        self._api_call_history[api_name] = [t for t in self._api_call_history[api_name] 
                                           if now - t < 60]

        # 检查当前窗口内的请求数量
        if len(self._api_call_history[api_name]) >= rate_limit:
            # 计算需要等待的时间
            oldest_call = min(self._api_call_history[api_name])
            wait_time = 60 - (now - oldest_call)

            if wait_time > 0:
                self.logger.debug(f"等待 {wait_time:.2f} 秒以遵守 {api_name} 的访问频率限制")
                time.sleep(wait_time)
                # 更新当前时间
                now = time.time()

        # 记录本次调用时间
        self._api_call_history[api_name].append(now)

    def _format_rows(self, fields, items, return_type: str = "pandas"):
        """Convert API rows to the requested return type."""
        self._validate_return_type(return_type)
        normalized_fields = list(fields or [])
        normalized_items = list(items or [])

        if return_type == "raw":
            return {"fields": normalized_fields, "items": normalized_items}

        frame = pd.DataFrame(normalized_items, columns=normalized_fields)

        if return_type == "pandas":
            return frame

        if return_type == "polars":
            try:
                import polars as pl
            except ImportError as exc:
                raise ImportError("return_type='polars' requires the optional 'polars' package") from exc
            return pl.from_pandas(frame)

        try:
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError("return_type='arrow' requires the optional 'pyarrow' package") from exc
        return pa.Table.from_pandas(frame, preserve_index=False)

    def _format_response_data(self, data: Dict[str, Any], return_type: str = "pandas"):
        """Convert one API response payload to the requested return type."""
        self._validate_return_type(return_type)
        if return_type == "raw":
            return dict(data)
        return self._format_rows(data.get("fields", []), data.get("items", []), return_type)

    def _validate_return_type(self, return_type: str) -> None:
        if return_type not in {"pandas", "polars", "arrow", "raw"}:
            raise ValueError("return_type must be one of: pandas, polars, arrow, raw")

    @staticmethod
    def _normalize_primary_key(primary_key):
        if primary_key is None:
            return ()
        if isinstance(primary_key, str):
            values = [value.strip() for value in primary_key.split(",")]
        else:
            values = [str(value).strip() for value in primary_key]
        normalized = tuple(value for value in values if value)
        if not normalized:
            raise ValueError("primary_key must contain at least one field")
        if len(set(normalized)) != len(normalized):
            raise ValueError("primary_key fields must be unique")
        return normalized

    @staticmethod
    def _page_key(value):
        """Return a hashable, deterministic representation of one key value."""
        try:
            hash(value)
            return value
        except TypeError:
            return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    @staticmethod
    def _page_content_signature(fields, items):
        """Fingerprint one response page to detect an ignored offset."""
        serialized = json.dumps(
            [fields, items],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=repr,
        ).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    def _validate_page_payload(
        self,
        data,
        expected_fields,
        strict_paging,
        report,
    ):
        if not isinstance(data, dict):
            raise PaginationProtocolError("page payload must be a mapping", report)
        if "fields" not in data or "items" not in data:
            raise PaginationProtocolError(
                "page payload must contain fields and items",
                report,
            )
        raw_fields = data.get("fields")
        raw_items = data.get("items")
        if not isinstance(raw_fields, (list, tuple)):
            raise PaginationProtocolError("page fields must be an array", report)
        if not all(isinstance(field, str) for field in raw_fields):
            raise PaginationProtocolError("page fields must contain strings", report)
        page_fields = list(raw_fields)
        if len(set(page_fields)) != len(page_fields):
            raise PaginationProtocolError("page fields must be unique", report)
        if not isinstance(raw_items, (list, tuple)):
            raise PaginationProtocolError("page items must be an array", report)
        page_items = list(raw_items)
        has_more = data.get("has_more", None)
        if strict_paging and has_more is not None and not isinstance(has_more, bool):
            raise PaginationProtocolError("has_more must be boolean or null", report)
        if expected_fields is not None and page_fields != expected_fields:
            raise PaginationProtocolError(
                "page fields changed across pagination: expected {0}, got {1}".format(
                    expected_fields,
                    page_fields,
                ),
                report,
            )
        for row in page_items:
            if not isinstance(row, (list, tuple)):
                raise PaginationProtocolError("page item must be a row sequence", report)
            row_length = len(row)
            if row_length != len(page_fields):
                raise PaginationProtocolError(
                    "page row width does not match fields",
                    report,
                )
        return page_fields, page_items, has_more

    def _register_page_keys(
        self,
        fields,
        items,
        primary_key,
        seen_keys,
        report,
    ):
        if not primary_key:
            return None, None
        missing = [name for name in primary_key if name not in fields]
        if missing:
            raise PaginationProtocolError(
                "primary_key fields missing from response: {0}".format(missing),
                report,
            )
        indices = [fields.index(name) for name in primary_key]
        sensitive_positions = [_is_sensitive_key(name) for name in primary_key]
        first_key = None
        last_key = None
        for row in items:
            key = tuple(self._page_key(row[index]) for index in indices)
            audit_key = tuple(
                "***REDACTED***" if sensitive else value
                for value, sensitive in zip(key, sensitive_positions)
            )
            if first_key is None:
                first_key = audit_key
            last_key = audit_key
            if key in seen_keys:
                report.duplicate_key_count += 1
                raise DuplicateKeyError(
                    "duplicate primary key across paged response: {0}".format(
                        audit_key
                    ),
                    report,
                )
            seen_keys.add(key)
        return first_key, last_key

    @staticmethod
    def _append_page_report(
        report,
        offset,
        requested_limit,
        row_count,
        has_more,
        first_key=None,
        last_key=None,
    ):
        page = {
            "offset": offset,
            "requested_limit": requested_limit,
            "row_count": row_count,
            "has_more": has_more,
        }
        if first_key is not None:
            page["first_key"] = list(first_key)
            page["last_key"] = list(last_key)
        report.pages.append(page)

    @staticmethod
    def _with_pagination_report(value, report, return_report):
        if return_report:
            return value, report
        return value

    def get_data(
        self,
        api_name,
        fields="",
        auto_paging=True,
        concurrent=False,
        max_pages=None,
        limit_per_request: Optional[int] = None,
        detect_limit: bool = True,
        return_type: str = "pandas",
        primary_key=None,
        strict_paging: bool = True,
        return_report: bool = False,
        **params
    ):
        """
        获取接口数据并按指定格式返回
        
        参数:
            api_name: API接口名称
            fields: 需要获取的字段，逗号分隔的字符串
            auto_paging: 是否自动处理分页
            concurrent: 是否使用并发请求
            max_pages: 最大分页数量；达到上限但未证明取尽时严格失败
            limit_per_request: 手工指定单次分页大小，指定后不会触发限制探测
            detect_limit: 是否自动探测单次请求限制；为False且未指定limit_per_request时使用5000
            return_type: 返回类型，支持 pandas、polars、arrow、raw；默认pandas
            primary_key: 可选主键字段；提供后跨页重复键会失败
            strict_paging: 是否对分页协议矛盾和未完整终止失败关闭
            return_report: 为True时返回 ``(data, PaginationReport)``
            **params: API的其他参数
        
        返回:
            默认返回原数据类型；return_report=True时返回数据与分页报告二元组
        """
        self._validate_return_type(return_type)
        normalized_key = self._normalize_primary_key(primary_key)

        if max_pages is not None:
            if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
                raise ValueError("max_pages must be a positive integer")

        start_offset = params.get("offset", 0)
        if isinstance(start_offset, bool) or not isinstance(start_offset, int) or start_offset < 0:
            raise ValueError("offset must be a non-negative integer")
        user_limit = params.get("limit", None)
        if user_limit is not None:
            if isinstance(user_limit, bool) or not isinstance(user_limit, int) or user_limit < 0:
                raise ValueError("limit must be a non-negative integer")

        # 如果不需要自动分页，直接调用原始方法
        if not auto_paging:
            report = PaginationReport(
                api_name=api_name,
                mode="single",
                page_size=params.get("limit"),
                pages_requested=1,
                start_offset=start_offset,
                user_limit=user_limit,
                max_pages=max_pages,
            )
            data = self._make_request(api_name, params, fields)
            page_fields, page_items, has_more = self._validate_page_payload(
                data,
                None,
                strict_paging,
                report,
            )
            first_key, last_key = self._register_page_keys(
                page_fields,
                page_items,
                normalized_key,
                set(),
                report,
            )
            report.pages_completed = 1
            report.rows_fetched = len(page_items)
            report.last_has_more = has_more
            report.request_satisfied = True
            report.source_exhausted = has_more is False
            report.complete = report.request_satisfied
            report.termination_reason = (
                "server_exhausted" if report.source_exhausted else "auto_paging_disabled"
            )
            self._append_page_report(
                report,
                start_offset,
                params.get("limit"),
                len(page_items),
                has_more,
                first_key,
                last_key,
            )
            explicit_limit_contradiction = (
                user_limit is not None
                and (
                    len(page_items) > user_limit
                    or (len(page_items) < user_limit and has_more is True)
                )
            )
            if explicit_limit_contradiction:
                report.request_satisfied = False
                report.complete = False
                report.source_exhausted = False
                report.exhaustion_inferred = False
                report.termination_reason = "protocol_contradiction"
                if strict_paging:
                    raise PaginationProtocolError(
                        "single request response contradicts its explicit limit",
                        report,
                    )
            value = self._format_response_data(data, return_type)
            return self._with_pagination_report(value, report, return_report)

        # 获取接口的单次传输限制；大表生产任务可显式传入以跳过昂贵探测。
        if limit_per_request is None:
            if detect_limit:
                probe_params = params.copy()
                probe_params.pop("offset", None)
                probe_params.pop("limit", None)
                api_info = self.get_api_info(
                    api_name,
                    probe_params=probe_params,
                    fields=fields,
                )
                limit_per_request = api_info.get('limit_per_request', 5000)
            else:
                limit_per_request = 5000

        if (
            isinstance(limit_per_request, bool)
            or not isinstance(limit_per_request, int)
            or limit_per_request < 0
        ):
            raise ValueError("limit_per_request must be a non-negative integer")

        # Explicit 0 is retained for compatibility, but automatic detection no
        # longer emits it because a narrow exhausted query does not prove that
        # an endpoint is unlimited.
        if limit_per_request == 0:
            report = PaginationReport(
                api_name=api_name,
                mode="single_unbounded",
                page_size=0,
                pages_requested=1,
                start_offset=start_offset,
                user_limit=user_limit,
                max_pages=max_pages,
            )
            data = self._make_request(api_name, params, fields)
            page_fields, page_items, has_more = self._validate_page_payload(
                data,
                None,
                strict_paging,
                report,
            )
            first_key, last_key = self._register_page_keys(
                page_fields,
                page_items,
                normalized_key,
                set(),
                report,
            )
            report.pages_completed = 1
            report.rows_fetched = len(page_items)
            report.last_has_more = has_more
            report.source_exhausted = has_more is False
            report.request_satisfied = report.source_exhausted
            report.complete = report.request_satisfied
            report.termination_reason = (
                "server_exhausted" if has_more is False else "unbounded_not_verified"
            )
            self._append_page_report(
                report,
                start_offset,
                params.get("limit"),
                len(page_items),
                has_more,
                first_key,
                last_key,
            )
            if strict_paging and has_more is True:
                raise PaginationIncompleteError(
                    "explicit unbounded request returned has_more=True",
                    report,
                )
            if strict_paging and has_more is None:
                raise PaginationIncompleteError(
                    "explicit unbounded request did not prove source exhaustion",
                    report,
                )
            value = self._format_response_data(data, return_type)
            return self._with_pagination_report(value, report, return_report)

        if user_limit == 0:
            report = PaginationReport(
                api_name=api_name,
                mode="concurrent" if concurrent else "sequential",
                page_size=limit_per_request,
                termination_reason="user_limit",
                complete=True,
                request_satisfied=True,
                start_offset=start_offset,
                user_limit=0,
                max_pages=max_pages,
            )
            value = self._format_rows([], [], return_type)
            return self._with_pagination_report(value, report, return_report)

        # 如果是并发模式，需要预先确定页数
        if concurrent:
            if max_pages is None:
                # 如果用户指定了limit，计算需要的页数
                if user_limit is not None:
                    max_pages = (user_limit + limit_per_request - 1) // limit_per_request
                else:
                    # A finite safety bound is necessary because page offsets
                    # must be planned before concurrent submission.
                    max_pages = 1000
                    self.logger.warning(f"并发模式下未指定max_pages或limit，默认尝试获取{max_pages}页数据")

            # 准备分页参数
            page_params = []
            for page in range(max_pages):
                page_offset = start_offset + page * limit_per_request

                # 如果用户指定了limit，确保不超过用户指定的总量
                if user_limit is not None:
                    remaining = user_limit - page * limit_per_request
                    if remaining <= 0:
                        break
                    page_limit = min(limit_per_request, remaining)
                else:
                    page_limit = limit_per_request

                page_param = params.copy()
                page_param['offset'] = page_offset
                page_param['limit'] = page_limit
                page_params.append((api_name, page_param, fields))

            # 使用并发请求
            return self._get_data_concurrent(
                page_params,
                return_type=return_type,
                primary_key=normalized_key,
                strict_paging=strict_paging,
                return_report=return_report,
                user_limit=user_limit,
                max_pages=max_pages,
                page_size=limit_per_request,
                start_offset=start_offset,
            )
        else:
            # 顺序模式，循环获取所有数据
            all_data = []
            fields_list = None
            total_fetched = 0
            offset = start_offset
            seen_keys = set()
            seen_page_signatures = {}
            report = PaginationReport(
                api_name=api_name,
                mode="sequential",
                page_size=limit_per_request,
                start_offset=start_offset,
                user_limit=user_limit,
                max_pages=max_pages,
            )

            while True:
                if max_pages is not None and report.pages_completed >= max_pages:
                    report.termination_reason = "max_pages"
                    report.complete = False
                    report.request_satisfied = False
                    if strict_paging:
                        raise PaginationIncompleteError(
                            "max_pages reached before pagination exhaustion",
                            report,
                        )
                    break

                # 复制参数，设置当前页的offset和limit
                page_params = params.copy()
                page_params['offset'] = offset

                # 如果用户指定了limit，确保不超过用户指定的总量
                if user_limit is not None:
                    remaining = user_limit - total_fetched
                    if remaining <= 0:
                        break
                    page_params['limit'] = min(limit_per_request, remaining)
                else:
                    page_params['limit'] = limit_per_request

                # 请求当前页数据
                self.logger.info(f"请求 {api_name} 数据: offset={offset}, limit={page_params['limit']}")
                report.pages_requested += 1
                data = self._make_request(api_name, page_params, fields)
                page_fields, page_items, has_more = self._validate_page_payload(
                    data,
                    fields_list,
                    strict_paging,
                    report,
                )
                if fields_list is None:
                    fields_list = page_fields
                current_count = len(page_items)
                if strict_paging and current_count > page_params["limit"]:
                    raise PaginationProtocolError(
                        "server returned more rows than requested",
                        report,
                    )
                request_will_continue = (
                    current_count > 0
                    and has_more is not False
                    and not (
                        user_limit is not None
                        and total_fetched + current_count >= user_limit
                    )
                )
                if request_will_continue:
                    page_signature = self._page_content_signature(
                        page_fields,
                        page_items,
                    )
                    previous_offset = seen_page_signatures.get(page_signature)
                    if previous_offset is not None and previous_offset != offset:
                        report.complete = False
                        report.request_satisfied = False
                        report.source_exhausted = False
                        report.exhaustion_inferred = False
                        report.termination_reason = "protocol_contradiction"
                        self._append_page_report(
                            report,
                            offset,
                            page_params["limit"],
                            current_count,
                            has_more,
                        )
                        raise PaginationProtocolError(
                            "server repeated an identical page at a new offset",
                            report,
                        )
                    seen_page_signatures[page_signature] = offset
                first_key, last_key = self._register_page_keys(
                    page_fields,
                    page_items,
                    normalized_key,
                    seen_keys,
                    report,
                )
                report.pages_completed += 1
                report.rows_fetched += current_count
                report.last_has_more = has_more
                self._append_page_report(
                    report,
                    offset,
                    page_params["limit"],
                    current_count,
                    has_more,
                    first_key,
                    last_key,
                )

                # 添加到结果集
                all_data.extend(page_items)
                total_fetched += current_count

                if current_count == 0:
                    if strict_paging and has_more is True:
                        raise PaginationProtocolError(
                            "empty page returned with has_more=True",
                            report,
                        )
                    report.source_exhausted = has_more is False
                    report.exhaustion_inferred = has_more is None
                    report.request_satisfied = True
                    report.complete = True
                    report.termination_reason = (
                        "server_exhausted"
                        if has_more is False
                        else "empty_page_inferred"
                    )
                    break

                # 优先使用has_more；老接口缺失该字段时，用短页判断结束。
                if has_more is False:
                    report.source_exhausted = True
                    report.request_satisfied = True
                    report.complete = True
                    report.termination_reason = "server_exhausted"
                    break

                # 更新offset，准备获取下一页
                offset += current_count

                # 如果用户指定了limit并且已经达到，停止获取
                if user_limit is not None and total_fetched >= user_limit:
                    report.request_satisfied = True
                    report.complete = True
                    report.termination_reason = "user_limit"
                    break

            self.logger.info(f"共获取 {len(all_data)} 条 {api_name} 数据")
            value = self._format_rows(fields_list, all_data, return_type)
            return self._with_pagination_report(value, report, return_report)

    def _get_data_concurrent(
        self,
        page_params,
        return_type="pandas",
        primary_key=(),
        strict_paging=True,
        return_report=False,
        user_limit=None,
        max_pages=None,
        page_size=None,
        start_offset=0,
    ):
        """并发请求多页数据"""
        fields = None
        all_data = []
        seen_keys = set()
        seen_page_signatures = {}
        api_name_for_report = page_params[0][0] if page_params else ""
        report = PaginationReport(
            api_name=api_name_for_report,
            mode="concurrent",
            page_size=page_size,
            start_offset=start_offset,
            user_limit=user_limit,
            max_pages=max_pages,
        )

        def fetch_page(params_tuple):
            api_name, params, field_str = params_tuple
            self.logger.info(f"并发请求 {api_name} 数据: offset={params.get('offset', 0)}, limit={params.get('limit', 0)}")
            return self._make_request(api_name, params, field_str)

        # 按照offset排序，确保从小到大处理
        sorted_params = sorted(page_params, key=lambda x: x[1].get('offset', 0))

        should_stop = False

        # 分批提交任务，而不是一次性提交所有任务
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            batch_size = self.max_workers  # 每批提交的任务数量

            for i in range(0, len(sorted_params), batch_size):
                if should_stop:
                    break

                # 获取当前批次的参数
                batch_params = sorted_params[i:i+batch_size]

                # 提交当前批次的任务
                future_to_params = {executor.submit(fetch_page, param): param for param in batch_params}
                report.pages_requested += len(future_to_params)

                batch_results = []
                for future in concurrent.futures.as_completed(future_to_params):
                    try:
                        data = future.result()
                        param = future_to_params[future]
                        batch_results.append((param[1], data, None))
                    except Exception as e:
                        param = future_to_params[future]
                        batch_results.append((param[1], None, e))

                # 按offset处理结果，避免as_completed导致返回顺序不稳定。
                terminal_offset = None
                for request_params, data, error in sorted(
                    batch_results,
                    key=lambda item: item[0].get("offset", 0),
                ):
                    offset = request_params.get("offset", 0)
                    requested_limit = request_params.get("limit", page_size)

                    # Requests later in the same concurrent batch are
                    # speculative.  Once a lower offset explicitly proves
                    # source exhaustion, an out-of-range/error at a higher
                    # offset is irrelevant.  A non-empty higher page, however,
                    # contradicts has_more=False and must never be discarded.
                    if terminal_offset is not None:
                        if error is not None:
                            if report.source_exhausted:
                                report.pages.append(
                                    {
                                        "offset": offset,
                                        "requested_limit": requested_limit,
                                        "row_count": None,
                                        "has_more": None,
                                        "ignored_after_source_exhaustion": True,
                                        "error_type": type(error).__name__,
                                    }
                                )
                                continue
                            raise error
                        extra_fields, extra_items, extra_has_more = self._validate_page_payload(
                            data,
                            fields,
                            strict_paging,
                            report,
                        )
                        report.pages_completed += 1
                        extra_page = {
                            "offset": offset,
                            "requested_limit": requested_limit,
                            "row_count": len(extra_items),
                            "has_more": extra_has_more,
                            "ignored_after_terminal": True,
                        }
                        report.pages.append(extra_page)
                        if extra_items or extra_has_more is True:
                            report.complete = False
                            report.request_satisfied = False
                            report.source_exhausted = False
                            report.exhaustion_inferred = False
                            report.termination_reason = "protocol_contradiction"
                            raise PaginationProtocolError(
                                "higher offset contradicted a lower page termination",
                                report,
                            )
                        continue

                    if error is not None:
                        safe_error = _redact_error_message(
                            error,
                            request_params,
                            extra_secrets=[self.token],
                        )
                        self.logger.error(
                            f"请求失败 {api_name_for_report}: {safe_error}"
                        )
                        raise error

                    page_fields, page_items, has_more = self._validate_page_payload(
                        data,
                        fields,
                        strict_paging,
                        report,
                    )
                    if fields is None:
                        fields = page_fields
                    current_count = len(page_items)
                    if strict_paging and current_count > requested_limit:
                        raise PaginationProtocolError(
                            "server returned more rows than requested",
                            report,
                        )
                    if strict_paging and current_count == 0 and has_more is True:
                        raise PaginationProtocolError(
                            "empty page returned with has_more=True",
                            report,
                        )
                    if (
                        strict_paging
                        and has_more is True
                        and current_count != requested_limit
                    ):
                        raise PaginationProtocolError(
                            "concurrent paging requires every non-final page to "
                            "match its requested size",
                            report,
                        )
                    if current_count > 0:
                        page_signature = self._page_content_signature(
                            page_fields,
                            page_items,
                        )
                        previous_offset = seen_page_signatures.get(page_signature)
                        if previous_offset is not None and previous_offset != offset:
                            report.complete = False
                            report.request_satisfied = False
                            report.source_exhausted = False
                            report.exhaustion_inferred = False
                            report.termination_reason = "protocol_contradiction"
                            self._append_page_report(
                                report,
                                offset,
                                requested_limit,
                                current_count,
                                has_more,
                            )
                            raise PaginationProtocolError(
                                "server repeated an identical page at a new offset",
                                report,
                            )
                        seen_page_signatures[page_signature] = offset
                    first_key, last_key = self._register_page_keys(
                        page_fields,
                        page_items,
                        primary_key,
                        seen_keys,
                        report,
                    )
                    report.pages_completed += 1
                    report.rows_fetched += current_count
                    report.last_has_more = has_more
                    self._append_page_report(
                        report,
                        offset,
                        requested_limit,
                        current_count,
                        has_more,
                        first_key,
                        last_key,
                    )
                    all_data.extend(page_items)

                    if has_more is False:
                        report.source_exhausted = True
                        report.request_satisfied = True
                        report.complete = True
                        report.termination_reason = "server_exhausted"
                        should_stop = True
                        terminal_offset = offset
                        continue
                    if user_limit is not None and len(all_data) >= user_limit:
                        report.request_satisfied = True
                        report.complete = True
                        report.termination_reason = "user_limit"
                        should_stop = True
                        terminal_offset = offset
                        continue
                    if has_more is None and current_count < requested_limit:
                        if current_count > 0:
                            report.termination_reason = "short_page_unverified"
                            if strict_paging:
                                raise PaginationProtocolError(
                                    "concurrent short page without has_more cannot "
                                    "prove a safe next offset",
                                    report,
                                )
                        else:
                            report.exhaustion_inferred = True
                            report.request_satisfied = True
                            report.complete = True
                            report.termination_reason = "empty_page_inferred"
                        should_stop = True
                        terminal_offset = offset
                        continue

        if not report.request_satisfied:
            if report.termination_reason is None:
                report.termination_reason = "max_pages"
            if strict_paging:
                raise PaginationIncompleteError(
                    "concurrent pagination ended before the request was satisfied",
                    report,
                )

        value = self._format_rows(fields, all_data, return_type)
        return self._with_pagination_report(value, report, return_report)

    def iter_data(
        self,
        api_name,
        param_chunks: Iterable[Dict],
        fields="",
        auto_paging=True,
        concurrent=False,
        max_pages=None,
        limit_per_request: Optional[int] = None,
        detect_limit: bool = True,
        return_type: str = "pandas",
        continue_on_error: bool = False,
        **base_params
    ):
        """逐个参数块拉取数据。

        该方法只提供通用执行原语，不内置任何接口或业务profile。调用方负责
        构造按日期、代码、月份或其他维度拆好的param_chunks。
        """
        for chunk_params in param_chunks:
            request_params = base_params.copy()
            request_params.update(chunk_params)
            try:
                frame = self.get_data(
                    api_name,
                    fields=fields,
                    auto_paging=auto_paging,
                    concurrent=concurrent,
                    max_pages=max_pages,
                    limit_per_request=limit_per_request,
                    detect_limit=detect_limit,
                    return_type=return_type,
                    **request_params
                )
                yield request_params, frame
            except Exception:
                if not continue_on_error:
                    raise
                self.logger.exception(
                    f"{api_name} 参数块请求失败，已跳过: "
                    f"{_redact_sensitive(request_params)}"
                )
                yield request_params, None

    def _default_partition_name(self, index: int, params: Dict, file_format: str) -> str:
        if not params:
            return f"part-{index:06d}.{file_format}"
        parts = [f"{_safe_filename_part(key)}={_safe_filename_part(value)}" for key, value in sorted(params.items())]
        return "__".join(parts) + f".{file_format}"

    def _resolve_partition_page_size(self, plan, combined_params):
        if not plan.auto_paging:
            return plan.limit_per_request
        if plan.limit_per_request is not None:
            return plan.limit_per_request
        if not plan.detect_limit:
            return 5000

        # Resolve once before partition workers start.  This prevents several
        # threads from racing on the same limit cache CSV.  Strict concurrent
        # paging will still reject any later partition whose effective server
        # page is smaller than this observed size, rather than skipping rows.
        probe_params = dict(combined_params)
        probe_params.pop("offset", None)
        probe_params.pop("limit", None)
        page_size = self._detect_request_limit(
            plan.api_name,
            probe_params,
            fields=plan.fields,
        )
        if not isinstance(page_size, int) or page_size <= 0:
            raise PaginationProtocolError(
                "limit detection did not produce a positive page size"
            )
        return page_size

    def _source_identity(self):
        """Return a credential-free identity for the concrete API source."""
        raw_api_url = str(self.api_url)
        parsed = urlsplit(raw_api_url)
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("api_url contains an invalid port")
        host = (parsed.hostname or "").lower()
        if ":" in host and not host.startswith("["):
            host = "[{0}]".format(host)
        netloc = host
        if port is not None:
            netloc = "{0}:{1}".format(netloc, port)
        api_origin = urlunsplit(
            (
                parsed.scheme.lower(),
                netloc,
                "",
                "",
                "",
            )
        )
        client_class = self.__class__
        auth_material = (
            b"tushare_plus.auth.v1\x00" + str(self.token).encode("utf-8")
        )
        return {
            "client_class": "{0}.{1}".format(
                client_class.__module__,
                getattr(client_class, "__qualname__", client_class.__name__),
            ),
            "api_origin": api_origin,
            "api_url_sha256": hashlib.sha256(
                raw_api_url.encode("utf-8")
            ).hexdigest(),
            "auth_identity_sha256": hashlib.sha256(auth_material).hexdigest(),
        }

    def _partition_query_identity(
        self,
        plan,
        params,
        effective_page_size,
        source_identity=None,
    ):
        return {
            "source_identity": (
                self._source_identity()
                if source_identity is None
                else copy.deepcopy(source_identity)
            ),
            "api_name": plan.api_name,
            "fields": plan.fields,
            "params": params,
            "file_format": plan.file_format,
            "pagination": {
                "auto_paging": plan.auto_paging,
                "concurrent": plan.concurrent,
                "max_pages": plan.max_pages,
                "limit_per_request": effective_page_size,
                "detect_limit": False,
                "primary_key": list(self._normalize_primary_key(plan.primary_key)),
                "strict_paging": plan.strict_paging,
            },
        }

    def _build_partition_specs(
        self,
        plan,
        effective_page_size,
        source_identity=None,
    ):
        output_path = plan.output_dir
        source_snapshot = (
            self._source_identity()
            if source_identity is None
            else copy.deepcopy(source_identity)
        )
        specs = []
        artifact_paths_seen = {}
        # Take one coherent deep snapshot so an external thread cannot mutate
        # nested plan parameters between two partition fingerprints.
        base_params_snapshot = copy.deepcopy(plan.base_params)
        chunks_snapshot = copy.deepcopy(plan.param_chunks)
        for index, chunk_params in enumerate(chunks_snapshot):
            request_params = copy.deepcopy(base_params_snapshot)
            request_params.update(chunk_params)
            redacted_params = _redact_sensitive(request_params)
            if plan.partition_filename is None:
                filename = self._default_partition_name(
                    index,
                    redacted_params,
                    plan.file_format,
                )
            else:
                # A custom callback receives redacted params so it cannot leak
                # credentials into a filename accidentally.
                filename = plan.partition_filename(index, redacted_params)
            if not isinstance(filename, str) or not filename:
                raise ValueError("partition filename must be a non-empty string")
            filename_path = Path(filename)
            if (
                filename_path.is_absolute()
                or len(filename_path.parts) != 1
                or filename_path.name in {".", ".."}
            ):
                raise ValueError(
                    "partition filename must be one relative filename inside output_dir"
                )
            path = output_path / filename_path.name
            sidecar_path = Path(str(path) + ".meta.json")
            for artifact_kind, artifact_path in (
                ("data", path),
                ("sidecar", sidecar_path),
            ):
                collision_key = os.path.normcase(
                    os.path.abspath(str(artifact_path))
                )
                if collision_key in artifact_paths_seen:
                    previous = artifact_paths_seen[collision_key]
                    raise ValueError(
                        "partition artifact collision between index {0} {1} "
                        "and index {2} {3}: {4}".format(
                            previous[0],
                            previous[1],
                            index,
                            artifact_kind,
                            artifact_path,
                        )
                    )
                artifact_paths_seen[collision_key] = (index, artifact_kind)
            query_identity = self._partition_query_identity(
                plan,
                request_params,
                effective_page_size,
                source_identity=source_snapshot,
            )
            artifact_secrets = _sensitive_values(request_params) + [self.token]
            specs.append(
                {
                    "index": index,
                    "params": request_params,
                    "redacted_params": redacted_params,
                    "path": path,
                    "sidecar_path": sidecar_path,
                    "source_identity": copy.deepcopy(source_snapshot),
                    "artifact_secrets": artifact_secrets,
                    "persisted_params": _scrub_secret_values(
                        redacted_params,
                        artifact_secrets,
                    ),
                    "query_fingerprint": _canonical_fingerprint(query_identity),
                }
            )
        return specs

    def _partition_plan_fingerprint(self, plan, specs, effective_page_size):
        payload = {
            "source_identity": (
                copy.deepcopy(specs[0]["source_identity"]) if specs else None
            ),
            "api_name": plan.api_name,
            "fields": plan.fields,
            "file_format": plan.file_format,
            "output_dir": os.path.abspath(str(plan.output_dir)),
            "effective_page_size": effective_page_size,
            "partitions": [
                {
                    "index": spec["index"],
                    "relative_path": spec["path"].name,
                    "query_fingerprint": spec["query_fingerprint"],
                }
                for spec in specs
            ],
        }
        return _canonical_fingerprint(payload)

    def _refresh_partition_query_fingerprints(
        self,
        plan,
        specs,
        effective_page_size,
    ):
        for spec in specs:
            query_identity = self._partition_query_identity(
                plan,
                spec["params"],
                effective_page_size,
                source_identity=spec["source_identity"],
            )
            spec["query_fingerprint"] = _canonical_fingerprint(query_identity)

    @staticmethod
    def _load_json_mapping(path):
        try:
            with open(str(path), "r", encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def _resume_partition(self, spec, plan, plan_fingerprint):
        path = spec["path"]
        sidecar_path = spec["sidecar_path"]
        if not path.exists() and not sidecar_path.exists():
            return None, None
        if not path.is_file() or not sidecar_path.is_file():
            raise PartitionCheckpointError(
                "resume found an orphan data file or sidecar; use resume=False "
                "to overwrite explicitly"
            )
        sidecar = self._load_json_mapping(sidecar_path)
        if sidecar is None:
            raise PartitionCheckpointError(
                "resume sidecar is unreadable; use resume=False to overwrite explicitly"
            )
        required = {
            "schema_version",
            "status",
            "api_name",
            "source_identity",
            "params",
            "fields",
            "file_format",
            "query_fingerprint",
            "plan_fingerprint",
            "row_count",
            "sha256",
            "created_at",
            "pagination_report",
        }
        if not required.issubset(set(sidecar)):
            raise PartitionCheckpointError(
                "resume sidecar contract is incomplete; use resume=False to overwrite"
            )
        if (
            sidecar.get("schema_version") != 1
            or sidecar.get("status") != "complete"
            or sidecar.get("api_name") != plan.api_name
            or sidecar.get("source_identity") != spec["source_identity"]
            or sidecar.get("params") != spec["persisted_params"]
            or sidecar.get("fields") != plan.fields
            or sidecar.get("file_format") != plan.file_format
            or sidecar.get("query_fingerprint") != spec["query_fingerprint"]
            or sidecar.get("plan_fingerprint") != plan_fingerprint
        ):
            raise PartitionCheckpointError(
                "resume artifact fingerprint or contract does not match the plan"
            )
        pagination = sidecar.get("pagination_report")
        if (
            not isinstance(pagination, dict)
            or pagination.get("request_satisfied") is not True
            or pagination.get("complete") is not True
        ):
            raise PartitionCheckpointError(
                "resume sidecar does not record a complete satisfied request"
            )
        row_count = sidecar.get("row_count")
        rows_fetched = pagination.get("rows_fetched")
        for name, value in (
            ("row_count", row_count),
            ("pagination rows_fetched", rows_fetched),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise PartitionCheckpointError(
                    "resume sidecar contains an invalid {0}".format(name)
                )
        if row_count != rows_fetched:
            raise PartitionCheckpointError(
                "resume sidecar row_count disagrees with pagination rows_fetched"
            )
        recorded_hash = sidecar.get("sha256")
        if not isinstance(recorded_hash, str) or re.match(
            r"^[0-9a-f]{64}$",
            recorded_hash,
        ) is None:
            raise PartitionCheckpointError("resume sidecar contains an invalid sha256")
        try:
            actual_hash = _sha256_file(path)
        except OSError:
            raise PartitionCheckpointError("resume data file cannot be hashed")
        if actual_hash != recorded_hash:
            # The signed contract identifies this exact query and plan, but
            # the data bytes are damaged.  It is safe to refetch atomically and
            # record the replacement in the execution manifest.
            return None, "invalid_checkpoint_replaced"
        return (
            PartitionResult(
                index=spec["index"],
                status="resumed",
                params=spec["params"],
                path=path,
                sidecar_path=sidecar_path,
                query_fingerprint=spec["query_fingerprint"],
                row_count=sidecar.get("row_count"),
                sha256=actual_hash,
                pagination_report=pagination,
                checkpoint_status="validated",
                artifact_secrets=spec["artifact_secrets"],
            ),
            None,
        )

    def _write_partition_atomically(
        self,
        frame,
        spec,
        plan,
        plan_fingerprint,
        pagination_report,
    ):
        path = spec["path"]
        sidecar_path = spec["sidecar_path"]
        data_temporary = _temporary_sibling(path)
        sidecar_temporary = _temporary_sibling(sidecar_path)
        try:
            if plan.file_format == "csv":
                frame.to_csv(data_temporary, index=False)
            else:
                frame.to_parquet(data_temporary, index=False)
            _fsync_file(data_temporary)
            digest = _sha256_file(data_temporary)
            safe_pagination_report = _scrub_secret_values(
                pagination_report,
                spec["artifact_secrets"],
            )
            sidecar = {
                "schema_version": 1,
                "status": "complete",
                "api_name": plan.api_name,
                "source_identity": copy.deepcopy(spec["source_identity"]),
                "params": copy.deepcopy(spec["persisted_params"]),
                "fields": plan.fields,
                "file_format": plan.file_format,
                "query_fingerprint": spec["query_fingerprint"],
                "plan_fingerprint": plan_fingerprint,
                "row_count": len(frame),
                "sha256": digest,
                "created_at": _utc_now_text(),
                "pagination_report": safe_pagination_report,
            }
            with open(str(sidecar_temporary), "w", encoding="utf-8") as handle:
                json.dump(
                    sidecar,
                    handle,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())

            # Replacing data first can leave data without its new sidecar if a
            # process dies between operations.  Resume detects that state as
            # an orphan (or a later hash mismatch) and never accepts it.
            os.replace(str(data_temporary), str(path))
            _fsync_directory(path.parent)
            os.replace(str(sidecar_temporary), str(sidecar_path))
            _fsync_directory(sidecar_path.parent)
            return digest, sidecar
        finally:
            for temporary in (data_temporary, sidecar_temporary):
                if temporary.exists():
                    temporary.unlink()

    def _execute_partition_spec(
        self,
        spec,
        plan,
        plan_fingerprint,
        effective_page_size,
        resume,
    ):
        try:
            checkpoint_status = None
            if self._source_identity() != spec["source_identity"]:
                raise PartitionCheckpointError(
                    "client source identity changed after partition planning"
                )
            if resume:
                resumed, checkpoint_status = self._resume_partition(
                    spec,
                    plan,
                    plan_fingerprint,
                )
                if resumed is not None:
                    return resumed
            frame, pagination = self.get_data(
                plan.api_name,
                fields=plan.fields,
                auto_paging=plan.auto_paging,
                concurrent=plan.concurrent,
                max_pages=plan.max_pages,
                limit_per_request=effective_page_size,
                detect_limit=False,
                return_type="pandas",
                primary_key=plan.primary_key,
                strict_paging=plan.strict_paging,
                return_report=True,
                **spec["params"]
            )
            pagination_dict = pagination.to_dict()
            if self._source_identity() != spec["source_identity"]:
                raise PartitionCheckpointError(
                    "client source identity changed during partition request"
                )
            if not pagination.request_satisfied:
                raise PaginationIncompleteError(
                    "partition request was not satisfied",
                    pagination,
                )
            if pagination.rows_fetched != len(frame):
                raise PaginationProtocolError(
                    "pagination rows_fetched disagrees with materialized rows",
                    pagination,
                )
            digest, sidecar = self._write_partition_atomically(
                frame,
                spec,
                plan,
                plan_fingerprint,
                pagination_dict,
            )
            return PartitionResult(
                index=spec["index"],
                status="written",
                params=spec["params"],
                path=spec["path"],
                sidecar_path=spec["sidecar_path"],
                query_fingerprint=spec["query_fingerprint"],
                row_count=len(frame),
                sha256=digest,
                pagination_report=sidecar["pagination_report"],
                checkpoint_status=checkpoint_status,
                artifact_secrets=spec["artifact_secrets"],
            )
        except Exception as error:
            pagination_report = None
            if isinstance(error, PaginationError) and error.report is not None:
                pagination_report = _scrub_secret_values(
                    error.report.to_dict(),
                    spec["artifact_secrets"],
                )
            return PartitionResult(
                index=spec["index"],
                status="failed",
                params=spec["params"],
                path=spec["path"],
                sidecar_path=spec["sidecar_path"],
                query_fingerprint=spec["query_fingerprint"],
                pagination_report=pagination_report,
                error_type=type(error).__name__,
                error_message=_redact_error_message(
                    error,
                    spec["params"],
                    extra_secrets=[self.token],
                ),
                artifact_secrets=spec["artifact_secrets"],
            )

    @staticmethod
    def _not_run_partition_result(spec):
        return PartitionResult(
            index=spec["index"],
            status="not_run",
            params=spec["params"],
            path=spec["path"],
            sidecar_path=spec["sidecar_path"],
            query_fingerprint=spec["query_fingerprint"],
            error_type=None,
            error_message="not executed after an earlier partition failure",
            artifact_secrets=spec["artifact_secrets"],
        )

    def execute_partition_plan(
        self,
        plan,
        resume=True,
        continue_on_error=False,
        manifest_path=None,
    ):
        """Execute a recoverable partition plan and write an audit manifest."""
        if not isinstance(plan, PartitionPlan):
            raise TypeError("plan must be a PartitionPlan")
        if not isinstance(resume, bool) or not isinstance(continue_on_error, bool):
            raise ValueError("resume and continue_on_error must be boolean")
        if plan.concurrent and plan.partition_workers * self.max_workers > 64:
            raise ValueError(
                "nested partition/page concurrency exceeds the safety limit of 64"
            )

        # Build every path and canonicalize every request before any probe,
        # filesystem write or data request.  The provisional page size is
        # refreshed under the execution lock after limit detection.
        source_identity = self._source_identity()
        specs = self._build_partition_specs(
            plan,
            None,
            source_identity=source_identity,
        )
        planning_fingerprint = self._partition_plan_fingerprint(
            plan,
            specs,
            None,
        )

        output_path = plan.output_dir
        if manifest_path is None:
            manifest = output_path / "execution_manifest.json"
        else:
            manifest = Path(manifest_path)
        output_lock = output_path / ".tushare_plus.run.lock"
        manifest_lock = manifest.parent / (".{0}.lock".format(manifest.name))
        manifest_collision = os.path.normcase(os.path.abspath(str(manifest)))
        artifact_paths = {
            os.path.normcase(os.path.abspath(str(spec["path"]))) for spec in specs
        }
        artifact_paths.update(
            os.path.normcase(os.path.abspath(str(spec["sidecar_path"])))
            for spec in specs
        )
        if manifest_collision in artifact_paths:
            raise ValueError("manifest_path collides with a partition artifact")
        for lock_path in (output_lock, manifest_lock):
            lock_collision = os.path.normcase(os.path.abspath(str(lock_path)))
            if lock_collision in artifact_paths or lock_collision == manifest_collision:
                raise ValueError("execution lock path collides with a partition artifact")

        # All planning and collision checks are complete before filesystem or
        # network mutation begins.
        output_path.mkdir(parents=True, exist_ok=True)
        manifest.parent.mkdir(parents=True, exist_ok=True)
        lock_owner, lock_paths = _acquire_execution_locks(
            (output_lock, manifest_lock),
            planning_fingerprint,
        )
        try:
            effective_page_size = self._resolve_partition_page_size(
                plan,
                specs[0]["params"],
            )
            if self._source_identity() != source_identity:
                raise PartitionCheckpointError(
                    "client source identity changed during limit detection"
                )
            self._refresh_partition_query_fingerprints(
                plan,
                specs,
                effective_page_size,
            )
            plan_fingerprint = self._partition_plan_fingerprint(
                plan,
                specs,
                effective_page_size,
            )
            started_at = _utc_now_text()
            results_by_index = {}

            if plan.partition_workers == 1:
                stopped = False
                for spec in specs:
                    if stopped:
                        results_by_index[spec["index"]] = (
                            self._not_run_partition_result(spec)
                        )
                        continue
                    result = self._execute_partition_spec(
                        spec,
                        plan,
                        plan_fingerprint,
                        effective_page_size,
                        resume,
                    )
                    results_by_index[result.index] = result
                    if result.status == "failed" and not continue_on_error:
                        stopped = True
            else:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=plan.partition_workers
                ) as executor:
                    futures = {
                        executor.submit(
                            self._execute_partition_spec,
                            spec,
                            plan,
                            plan_fingerprint,
                            effective_page_size,
                            resume,
                        ): spec
                        for spec in specs
                    }
                    cancellation_requested = False
                    for future in concurrent.futures.as_completed(futures):
                        if future.cancelled():
                            continue
                        result = future.result()
                        results_by_index[result.index] = result
                        if (
                            result.status == "failed"
                            and not continue_on_error
                            and not cancellation_requested
                        ):
                            cancellation_requested = True
                            for other_future in futures:
                                if other_future is not future:
                                    other_future.cancel()

                    for future, spec in futures.items():
                        if spec["index"] not in results_by_index:
                            results_by_index[spec["index"]] = (
                                self._not_run_partition_result(spec)
                            )

            ordered_results = [
                results_by_index[index] for index in range(len(specs))
            ]
            execution = PartitionExecutionResult(
                api_name=plan.api_name,
                plan_fingerprint=plan_fingerprint,
                started_at=started_at,
                finished_at=_utc_now_text(),
                partitions=ordered_results,
                manifest_path=manifest,
            )
            _atomic_write_json(manifest, execution.to_dict())
            if not execution.complete and not continue_on_error:
                raise PartitionExecutionError(execution)
            return execution
        finally:
            _release_execution_locks(lock_owner, lock_paths)

    def download_partitions(
        self,
        api_name,
        param_chunks: Iterable[Dict],
        output_dir,
        fields="",
        file_format="csv",
        partition_filename: Optional[Callable[[int, Dict], str]] = None,
        skip_existing: bool = True,
        auto_paging=True,
        concurrent=False,
        max_pages=None,
        limit_per_request: Optional[int] = None,
        detect_limit: bool = True,
        continue_on_error: bool = False,
        primary_key=None,
        strict_paging: bool = True,
        partition_workers: int = 1,
        manifest_path=None,
        **base_params
    ) -> List[Path]:
        """按参数块下载并落盘。

        向后兼容的便捷包装；执行由 ``PartitionPlan`` 引擎负责。现有文件
        只有在 sidecar、请求/计划指纹和 SHA-256 全部匹配时才会跳过。
        """
        chunks = list(param_chunks)
        if not chunks:
            return []
        plan = PartitionPlan(
            api_name=api_name,
            param_chunks=chunks,
            output_dir=output_dir,
            fields=fields,
            file_format=file_format,
            base_params=base_params,
            partition_filename=partition_filename,
            auto_paging=auto_paging,
            concurrent=concurrent,
            max_pages=max_pages,
            limit_per_request=limit_per_request,
            detect_limit=detect_limit,
            primary_key=primary_key,
            strict_paging=strict_paging,
            partition_workers=partition_workers,
        )
        execution = self.execute_partition_plan(
            plan,
            resume=skip_existing,
            continue_on_error=continue_on_error,
            manifest_path=manifest_path,
        )
        return execution.paths


class DataCubeAPI(TushareAPI):
    """
    用于访问类似Tushare的数据源的客户端，方正DataCube。
    默认禁用API访问频率限制。
    """
    def __init__(
        self,
        token=None,
        max_workers=5,
        max_retries=3,
        retry_delay=1,
        retry_backoff=2.0,
        retry_jitter=0.1,
        max_retry_delay=60,
        request_timeout: Optional[float] = 60,
        custom_params_file=None,
        api_limits_file: Optional[str] = None,
        api_limits_default_filename: str = "datacube_api_limits.csv"
    ):

        if not token:
            token = os.environ.get('DATACUBE_TOKEN')

        if not token:
            raise ValueError("Token must be provided either as an argument or via DATACUBE_TOKEN environment variable.")

        # 调用父类的构造函数，并明确设置 enable_rate_limit=False
        super().__init__(
            token=token,
            max_workers=max_workers,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            retry_jitter=retry_jitter,
            max_retry_delay=max_retry_delay,
            request_timeout=request_timeout,
            enable_rate_limit=False,  # 禁用频率限制
            use_env_proxy=False,
            custom_params_file=custom_params_file,
            api_limits_file=api_limits_file,
            api_limits_default_filename=api_limits_default_filename
        )
        # 设置新的API URL
        self.api_url = "http://datacubeapi.foundersc.com"
        self._api_required_params['fund_nav'] = {'end_date': '20250506'}  # 方正DataCube的必要参数
