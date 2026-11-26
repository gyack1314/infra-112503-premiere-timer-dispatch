"""IANA 时区、fold 与 UTC 秒点转换工具。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class TimePointError(ValueError):
    """时点无法按指定 IANA 时区解释。"""


def parse_iso_utc(value: str) -> int:
    """把 ISO-8601 UTC 时间解析为整数 POSIX/UTC 秒点。"""
    if not isinstance(value, str) or not value:
        raise TimePointError("UTC 时间必须是非空 ISO 字符串")
    text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise TimePointError(f"无法解析 UTC 时间 {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def iso_utc(epoch: int | float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(int(epoch), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def local_is_ambiguous(naive: datetime, tz: ZoneInfo) -> bool:
    first = naive.replace(tzinfo=tz, fold=0)
    second = naive.replace(tzinfo=tz, fold=1)
    return first.utcoffset() != second.utcoffset()


def local_exists(naive: datetime, tz: ZoneInfo, fold: int = 0) -> bool:
    """用 UTC 往返检测春令时中不存在的本地时刻。"""
    aware = naive.replace(tzinfo=tz, fold=fold)
    round_trip = aware.astimezone(timezone.utc).astimezone(tz)
    return round_trip.replace(tzinfo=None) == naive


@dataclass(frozen=True)
class ResolvedLocalTime:
    epoch: int
    utc: str
    utc_offset_seconds: int
    ambiguous: bool
    fold: int

    def as_dict(self) -> dict:
        return {
            "utc_epoch": self.epoch,
            "utc": self.utc,
            "utc_offset_seconds": self.utc_offset_seconds,
            "ambiguous": self.ambiguous,
            "fold": self.fold,
        }


def resolve_local_time(local: object, tz_name: object, fold: object = None) -> ResolvedLocalTime:
    """把地区本地时刻解析为明确 UTC 秒点。

    - tz_name 必须是可加载的 IANA 名；
    - local 必须是不带偏移的完整本地时间；
    - 夏令时回拨产生的歧义时刻必须显式给 fold=0/1；
    - 春令时跳空产生的不存在时刻直接报错，交由编排修改。
    """
    if not isinstance(tz_name, str) or not tz_name:
        raise TimePointError("tz 必须是非空 IANA 时区名")
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise TimePointError(f"未知 IANA 时区 {tz_name!r}") from exc

    if not isinstance(local, str) or not local:
        raise TimePointError("local 必须是非空完整本地时间")
    try:
        naive = datetime.fromisoformat(local)
    except ValueError as exc:
        raise TimePointError(f"无法解析本地时间 {local!r}") from exc

    if naive.tzinfo is not None:
        raise TimePointError("local 不能带偏移；偏移必须由 tz 与 fold 明确决定")

    ambiguous = local_is_ambiguous(naive, tz)
    if ambiguous:
        if fold is None:
            raise TimePointError("该本地时刻处于夏令时回拨歧义区间，必须显式指定 fold=0（第一次）或 fold=1（第二次）")
        if fold not in (0, 1):
            raise TimePointError("fold 只能是 0 或 1")
        selected_fold = int(fold)
    else:
        if fold is not None and fold not in (0, 1):
            raise TimePointError("fold 只能是 0 或 1")
        selected_fold = int(fold) if fold in (0, 1) else 0

    if not local_exists(naive, tz, selected_fold):
        raise TimePointError("该本地时刻在夏令时跳空中不存在，请编排修改时点")

    aware = naive.replace(tzinfo=tz, fold=selected_fold)
    offset = aware.utcoffset()
    offset_seconds = int(offset.total_seconds()) if offset is not None else 0
    epoch = int(aware.timestamp())
    return ResolvedLocalTime(
        epoch=epoch,
        utc=iso_utc(epoch) or "",
        utc_offset_seconds=offset_seconds,
        ambiguous=ambiguous,
        fold=selected_fold,
    )
