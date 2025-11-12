from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import boto3

from .logging_setup import get_logger

logger = get_logger()


@dataclass
class _UploadPart:
    part_number: int
    etag: str


class WindowedS3Writer:
    def __init__(
        self,
        *,
        bucket: str,
        base_prefix: str,
        window_minutes: int = 30,
        max_object_bytes: int = 512_000_000,
        part_size_bytes: int = 16_777_216,
        aws_region_name: Optional[str] = None,
        use_marker: bool = True,
    ) -> None:
        self.bucket = bucket
        self.base_prefix = base_prefix.strip("/")
        self.window_minutes = window_minutes
        self.max_object_bytes = max_object_bytes
        self.part_size_bytes = max(part_size_bytes, 5 * 1024 * 1024)
        self.use_marker = use_marker
        self.hostname = socket.gethostname()

        session = boto3.session.Session(region_name=aws_region_name)
        self.s3 = session.client("s3")

        self._current_window_start: Optional[datetime] = None
        self._current_window_end: Optional[datetime] = None
        self._seq_counter: int = 0

        self._buffer = bytearray()
        self._object_bytes_streamed = 0
        self._upload_id: Optional[str] = None
        self._parts: List[_UploadPart] = []
        self._object_key: Optional[str] = None

    def _floor_window(self, when: datetime) -> datetime:
        minute = (when.minute // self.window_minutes) * self.window_minutes
        return when.replace(minute=minute, second=0, microsecond=0, tzinfo=timezone.utc)

    def _window_bounds(self, when: datetime) -> tuple[datetime, datetime]:
        start = self._floor_window(when)
        end = start + timedelta(minutes=self.window_minutes) - timedelta(seconds=1)
        return start, end

    def _prefix_for_window(self, start: datetime, end: datetime) -> str:
        date_prefix = start.strftime("ingest_dt=%Y/%m/%d/hour=%H")
        part_prefix = f"part={start.strftime('%Y%m%d_%H%M')}-{end.strftime('%Y%m%d_%H%M')}"
        return f"{self.base_prefix}/{date_prefix}/{part_prefix}"

    def _object_name(self) -> str:
        self._seq_counter += 1
        return f"{self.hostname}-seq={self._seq_counter:06d}.ndjson"

    def _current_key(self) -> str:
        assert self._object_key is not None
        return self._object_key

    def _start_new_window(self, when: datetime) -> None:
        start, end = self._window_bounds(when)
        prefix = self._prefix_for_window(start, end)
        key = f"{prefix}/{self._object_name()}"
        self._current_window_start = start
        self._current_window_end = end
        self._object_key = key
        self._buffer = bytearray()
        self._object_bytes_streamed = 0
        self._parts.clear()
        if self.use_marker:
            marker = f"{prefix}/uploading.marker"
            try:
                self.s3.put_object(Bucket=self.bucket, Key=marker, Body=b"")
            except Exception as exc:  # noqa: BLE001
                logger.warning("s3_marker_write_failed", key=marker, error=str(exc))
        resp = self.s3.create_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            ContentType="application/x-ndjson",
        )
        self._upload_id = resp["UploadId"]

    def _should_rotate(self, when: datetime) -> bool:
        if self._current_window_end is None:
            return True
        if when > self._current_window_end:
            return True
        if self._object_bytes_streamed >= self.max_object_bytes:
            return True
        return False

    def _upload_available_parts(self) -> None:
        while len(self._buffer) >= self.part_size_bytes:
            data = bytes(self._buffer[: self.part_size_bytes])
            del self._buffer[: self.part_size_bytes]
            part_num = len(self._parts) + 1
            resp = self.s3.upload_part(
                Bucket=self.bucket,
                Key=self._current_key(),
                PartNumber=part_num,
                UploadId=str(self._upload_id),
                Body=data,
            )
            self._parts.append(_UploadPart(part_number=part_num, etag=resp["ETag"]))

    def write_line(self, line: str) -> None:
        now = datetime.now(timezone.utc)
        if self._should_rotate(now):
            self._finalize_current()
            self._start_new_window(now)
        data = (line + "\n").encode("utf-8") if not line.endswith("\n") else line.encode("utf-8")
        self._buffer.extend(data)
        self._object_bytes_streamed += len(data)
        self._upload_available_parts()

    def _finalize_current(self) -> None:
        if self._upload_id is None:
            return
        try:
            if self._buffer:
                data = bytes(self._buffer)
                part_num = len(self._parts) + 1
                resp = self.s3.upload_part(
                    Bucket=self.bucket,
                    Key=self._current_key(),
                    PartNumber=part_num,
                    UploadId=str(self._upload_id),
                    Body=data,
                )
                self._parts.append(_UploadPart(part_number=part_num, etag=resp["ETag"]))
            parts_payload = {"Parts": [{"ETag": p.etag, "PartNumber": p.part_number} for p in self._parts]}
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self._current_key(),
                UploadId=str(self._upload_id),
                MultipartUpload=parts_payload,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("s3_finalize_failed", key=self._object_key, error=str(exc))
            try:
                if self._upload_id:
                    self.s3.abort_multipart_upload(Bucket=self.bucket, Key=self._current_key(), UploadId=str(self._upload_id))
            except Exception as abort_exc:  # noqa: BLE001
                logger.warning("s3_abort_failed", key=self._object_key, error=str(abort_exc))
        finally:
            if self.use_marker and self._current_window_start and self._current_window_end:
                prefix = self._prefix_for_window(self._current_window_start, self._current_window_end)
                marker = f"{prefix}/uploading.marker"
                try:
                    self.s3.delete_object(Bucket=self.bucket, Key=marker)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("s3_marker_delete_failed", key=marker, error=str(exc))
            self._buffer = bytearray()
            self._object_bytes_streamed = 0
            self._upload_id = None
            self._parts.clear()
            self._object_key = None

    def close(self) -> None:
        self._finalize_current()


class AsyncS3Writer:
    def __init__(self, writer: WindowedS3Writer) -> None:
        self._writer = writer

    async def write_line(self, line: str) -> None:
        await __import__("asyncio").to_thread(self._writer.write_line, line)

    async def close(self) -> None:
        await __import__("asyncio").to_thread(self._writer.close)


class S3RawMultiWriter:
    def __init__(
        self,
        *,
        bucket: str,
        base_prefix: str,
        aws_region_name: Optional[str],
        window_minutes: int,
        max_object_bytes: int,
        part_size_bytes: int,
        use_marker: bool,
    ) -> None:
        self._writers: Dict[str, AsyncS3Writer] = {}
        for channel in ("AM", "FMV", "T", "Q"):
            w = WindowedS3Writer(
                bucket=bucket,
                base_prefix=f"{base_prefix}/channel={channel}",
                window_minutes=window_minutes,
                max_object_bytes=max_object_bytes,
                part_size_bytes=part_size_bytes,
                aws_region_name=aws_region_name,
                use_marker=use_marker,
            )
            self._writers[channel] = AsyncS3Writer(w)

    async def write(self, channel: str, line: str) -> None:
        ch = channel.upper()
        writer = self._writers.get(ch)
        if writer is not None:
            await writer.write_line(line)

    async def close(self) -> None:
        for w in self._writers.values():
            await w.close()
