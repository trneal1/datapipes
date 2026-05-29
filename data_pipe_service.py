#!/usr/bin/env python3
"""
Data Pipe Service

Features:
- Multiple configurable input pipes.
- Each pipe can receive pushed TCP/UDP records or periodically pull records from HTTP.
- TCP and UDP input pipes listen on their own port.
- Incoming record delimiter is configurable per pipe; default is \n.
- Outgoing record delimiter is configurable per pipe; default is \n.
- Each pipe may define one or more field delimiters.
- Delimiters, record delimiters, and strip characters support escape notation:
  \n, \r, \t, \x1e, etc.
- Each JSON field has a configured name and type: text or numeric.
- Configured strip characters are stripped from the beginning and end of each
  input field before JSON output or numeric conversion.
- Each pipe may define included CSV field numbers, such as 1-3,4,6. JSON field
  names and types are applied after this inclusion filter.
- Each enabled pipe keeps a persistent outgoing TCP connection to its endpoint.
- If the endpoint disconnects, the service keeps trying to reconnect.
- Web UI allows adding, editing, deleting, enabling, and disabling pipes.
- Web UI shows endpoint connection status and refreshes it automatically.
- Configuration persists in pipes_config.json between executions.

Run:
    pip install aiohttp
    python data_pipe_service.py

Open locally:
    http://127.0.0.1:8080

Open from another computer on the same network:
    http://<server-ip-address>:8080
"""

from __future__ import annotations

import asyncio
import ast
import codecs
import json
import operator
import re
import signal
import socket
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urlsplit, urlunsplit

from aiohttp import ClientSession, ClientTimeout, web


CONFIG_FILE = Path("pipes_config.json")
WEB_HOST = "0.0.0.0"
WEB_PORT = 8085

CONNECT_TIMEOUT_SECONDS = 3
RECONNECT_DELAY_SECONDS = 2
CLOSE_TIMEOUT_SECONDS = 1

ALLOWED_FIELD_TYPES = {"text", "numeric"}
ALLOWED_INPUT_MODES = {"tcp", "udp", "http_pull"}
ALLOWED_DELIMITER_MODES = {"literal", "regex"}
ALLOWED_JSON_TAG_SOURCES = {"constant", "field", "numeric", "text"}
ALLOWED_JSON_TAG_TRANSFORMS = {"none", "strip", "upper", "lower", "title", "lstrip", "rstrip"}
ALLOWED_JSON_TAG_VALUE_TYPES = {"text", "numeric"}
NUMERIC_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}
NUMERIC_UNARY_OPERATORS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def decode_escape_text(value: str) -> str:
    """Decode user-entered escape sequences like \\n, \\r, \\t, \\x1e."""
    if value is None:
        return ""
    return codecs.decode(str(value), "unicode_escape")


def encode_escape_text(value: str) -> str:
    """Encode special characters for display/API responses."""
    return value.encode("unicode_escape").decode("ascii")


def parse_escaped_lines(value: object, *, allow_space_word: bool = False) -> List[str]:
    """
    Parse a string/list of escape-capable values into decoded strings.

    Input can be:
    - list[str]
    - newline-separated text
    - comma-separated text

    Special strip-character convenience tokens:
    - space
    - <space>
    - (space)
    """
    if value is None:
        return []

    if isinstance(value, list):
        raw_items = [str(item) for item in value]
    else:
        text = str(value)
        if not text:
            return []
        if "\n" in text or "\r" in text:
            raw_items = text.splitlines()
        elif text == ",":
            raw_items = [","]
        else:
            raw_items = [item for item in text.split(",")]

    decoded: List[str] = []
    for raw in raw_items:
        item = raw.strip()
        if item == "":
            continue

        if allow_space_word and item.lower() in {"space", "<space>", "(space)"}:
            decoded_item = " "
        else:
            decoded_item = decode_escape_text(item)

        if decoded_item == "":
            raise ValueError("Delimiter/strip entries cannot decode to an empty string.")
        decoded.append(decoded_item)

    return decoded


def parse_included_fields(value: object) -> List[int]:
    """
    Parse a 1-based CSV field include expression into 0-based field indexes.

    Supported examples:
    - 1-3
    - 1-3,4,6,7
    - blank means include every parsed field
    """
    if value is None:
        return []

    text = str(value).strip()
    if not text:
        return []

    indexes: List[int] = []
    seen = set()

    for raw_part in text.split(","):
        part = raw_part.strip()
        if not part:
            continue

        if "-" in part:
            raw_start, raw_end = [item.strip() for item in part.split("-", 1)]
            if not raw_start or not raw_end:
                raise ValueError("Included fields ranges must use the form 1-3.")

            try:
                start = int(raw_start)
                end = int(raw_end)
            except ValueError as exc:
                raise ValueError("Included fields must be numbers or ranges like 1-3.") from exc

            if start < 1 or end < 1:
                raise ValueError("Included fields must be 1 or greater.")
            if start > end:
                raise ValueError("Included fields ranges must start before they end.")

            field_numbers = range(start, end + 1)
        else:
            try:
                field_number = int(part)
            except ValueError as exc:
                raise ValueError("Included fields must be numbers or ranges like 1-3.") from exc

            if field_number < 1:
                raise ValueError("Included fields must be 1 or greater.")

            field_numbers = [field_number]

        for field_number in field_numbers:
            index = field_number - 1
            if index in seen:
                raise ValueError(f"CSV field {field_number} is included more than once.")
            seen.add(index)
            indexes.append(index)

    return indexes


@dataclass
class PipeConfig:
    name: str
    input_mode: str = "tcp"
    listen_host: str = "0.0.0.0"
    listen_port: int = 9000
    outgoing_host: str = "127.0.0.1"
    outgoing_port: int = 9100
    http_url: str = ""
    http_urls: List[str] = field(default_factory=list)
    http_port: int = 0
    http_interval_seconds: int = 60
    http_timeout_seconds: int = 10

    # Backward-compatible single field delimiter.
    delimiter: str = ","
    delimiter_mode: str = "literal"

    # New multi-delimiter and record-delimiter settings.
    delimiters: List[str] = field(default_factory=list)
    incoming_record_delimiter: str = "\n"
    outgoing_record_delimiter: str = "\n"
    strip_chars: List[str] = field(default_factory=list)
    included_fields: str = ""

    field_names: List[str] = field(default_factory=list)
    field_types: List[str] = field(default_factory=list)
    json_tags: List[dict] = field(default_factory=list)

    enabled: bool = True

    def __post_init__(self) -> None:
        self.input_mode = str(self.input_mode or "tcp").strip()
        self.http_url = str(self.http_url or "").strip()
        self.http_urls = [
            str(url).strip()
            for url in self.http_urls
            if str(url).strip()
        ]
        if not self.http_urls and self.http_url:
            self.http_urls = [self.http_url]
        self.http_url = self.http_urls[0] if self.http_urls else ""

        if not self.delimiters:
            self.delimiters = [self.delimiter or ","]

        self.delimiter_mode = str(self.delimiter_mode or "literal").strip()
        if self.delimiter_mode == "literal":
            # Values may already be decoded from config, or escaped from API.
            self.delimiters = [decode_escape_text(d) for d in self.delimiters]
        else:
            self.delimiters = [str(d) for d in self.delimiters]
        self.incoming_record_delimiter = decode_escape_text(self.incoming_record_delimiter or "\\n")
        self.outgoing_record_delimiter = decode_escape_text(self.outgoing_record_delimiter or "\\n")
        self.strip_chars = [decode_escape_text(c) for c in self.strip_chars]
        self.included_fields = str(self.included_fields or "").strip()

        if not self.field_types:
            self.field_types = ["text"] * len(self.field_names)

        self.json_tags = [dict(tag) for tag in self.json_tags if isinstance(tag, dict)]

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("Pipe name is required.")

        if self.input_mode not in ALLOWED_INPUT_MODES:
            raise ValueError("Input mode must be tcp, udp, or http_pull.")

        if self.delimiter_mode not in ALLOWED_DELIMITER_MODES:
            raise ValueError("Field delimiter mode must be literal or regex.")

        if not self.delimiters:
            raise ValueError("At least one field delimiter is required.")

        for delimiter in self.delimiters:
            if delimiter == "":
                raise ValueError("Field delimiters cannot be empty.")

            if self.delimiter_mode == "regex":
                try:
                    compiled_delimiter = re.compile(delimiter)
                except re.error as exc:
                    raise ValueError(f"Invalid field delimiter regex {delimiter!r}: {exc}") from exc

                empty_match = compiled_delimiter.match("")
                if empty_match is not None and empty_match.end() == empty_match.start():
                    raise ValueError(
                        f"Field delimiter regex {delimiter!r} cannot match an empty string."
                    )

        if self.incoming_record_delimiter == "":
            raise ValueError("Incoming record delimiter cannot be empty.")

        if self.outgoing_record_delimiter == "":
            raise ValueError("Outgoing record delimiter cannot be empty.")

        self.listen_port = int(self.listen_port)
        self.outgoing_port = int(self.outgoing_port)
        self.http_port = int(self.http_port or 0)
        self.http_interval_seconds = int(self.http_interval_seconds)
        self.http_timeout_seconds = int(self.http_timeout_seconds)

        if self.input_mode in {"tcp", "udp"} and not (1 <= self.listen_port <= 65535):
            raise ValueError("Listen port must be between 1 and 65535.")

        if not (1 <= self.outgoing_port <= 65535):
            raise ValueError("Outgoing port must be between 1 and 65535.")

        if not (0 <= self.http_port <= 65535):
            raise ValueError("HTTP port must be 0 or between 1 and 65535.")

        if self.input_mode == "http_pull":
            if not self.http_urls:
                raise ValueError("At least one HTTP URL is required.")

            for http_url in self.http_urls:
                parsed_url = urlsplit(http_url)
                if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
                    raise ValueError("Every HTTP URL must be a full http:// or https:// URL.")

            if self.http_interval_seconds < 1:
                raise ValueError("HTTP pull interval must be at least 1 second.")

            if self.http_timeout_seconds < 1:
                raise ValueError("HTTP timeout must be at least 1 second.")

        if len(set(self.field_names)) != len(self.field_names):
            raise ValueError("JSON field names must be unique.")

        for field_name in self.field_names:
            if not field_name.strip():
                raise ValueError("JSON field names cannot be blank.")

        if len(self.field_types) != len(self.field_names):
            raise ValueError("Every JSON field must have a type.")

        for field_type in self.field_types:
            if field_type not in ALLOWED_FIELD_TYPES:
                raise ValueError("JSON field type must be text or numeric.")

        self.validate_json_tags()
        parse_included_fields(self.included_fields)

    def validate_json_tags(self) -> None:
        seen_names = set()
        field_name_set = set(self.field_names)

        for tag in self.json_tags:
            name = str(tag.get("name", "")).strip()
            if not name:
                raise ValueError("Extra JSON tag names cannot be blank.")
            if name in seen_names:
                raise ValueError(f"Extra JSON tag '{name}' is configured more than once.")
            if name in field_name_set:
                raise ValueError(f"Extra JSON tag '{name}' conflicts with a JSON field name.")
            seen_names.add(name)
            tag["name"] = name

            source = str(tag.get("source", "constant") or "constant")
            if source not in ALLOWED_JSON_TAG_SOURCES:
                raise ValueError("Extra JSON tag source must be constant, field, numeric, or text.")
            tag["source"] = source

            transform = str(tag.get("transform", "none") or "none")
            if transform not in ALLOWED_JSON_TAG_TRANSFORMS:
                raise ValueError("Extra JSON tag transform is invalid.")
            tag["transform"] = transform

            value_type = str(tag.get("value_type", "text") or "text")
            if value_type not in ALLOWED_JSON_TAG_VALUE_TYPES:
                raise ValueError("Extra JSON tag value type must be text or numeric.")
            tag["value_type"] = value_type

            if source == "field":
                field_number = int(tag.get("field_number") or 0)
                if field_number < 1:
                    raise ValueError(f"Extra JSON tag '{name}' field number must be 1 or greater.")
                tag["field_number"] = field_number
            elif source in {"numeric", "text", "constant"}:
                tag["value"] = str(tag.get("value", ""))

    def to_public_dict(self) -> dict:
        item = asdict(self)
        if self.delimiter_mode == "regex":
            item["delimiter"] = self.delimiters[0] if self.delimiters else ","
            item["delimiters"] = list(self.delimiters)
        else:
            item["delimiter"] = encode_escape_text(self.delimiters[0]) if self.delimiters else ","
            item["delimiters"] = [encode_escape_text(d) for d in self.delimiters]
        item["incoming_record_delimiter"] = encode_escape_text(self.incoming_record_delimiter)
        item["outgoing_record_delimiter"] = encode_escape_text(self.outgoing_record_delimiter)
        item["strip_chars"] = [encode_escape_text(c) for c in self.strip_chars]
        return item


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self.pipes: Dict[str, PipeConfig] = {}

    def load(self) -> None:
        if not self.path.exists():
            self.pipes = {}
            return

        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self.pipes = {
            item["name"]: PipeConfig(**item)
            for item in raw.get("pipes", [])
        }

    def save(self) -> None:
        data = {"pipes": [asdict(pipe) for pipe in self.pipes.values()]}
        temp_path = self.path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        temp_path.replace(self.path)

    def rename_or_upsert(self, original_name: str, pipe: PipeConfig) -> None:
        pipe.validate()
        if original_name and original_name != pipe.name:
            self.pipes.pop(original_name, None)
        self.pipes[pipe.name] = pipe
        self.save()

    def delete(self, name: str) -> None:
        self.pipes.pop(name, None)
        self.save()


class PipeRuntime:
    def __init__(self, config: PipeConfig, record_count: int = 0):
        self.config = config
        self.server: Optional[asyncio.AbstractServer] = None
        self.udp_transport: Optional[asyncio.DatagramTransport] = None

        self.outgoing_reader: Optional[asyncio.StreamReader] = None
        self.outgoing_writer: Optional[asyncio.StreamWriter] = None
        self.outgoing_connected = False
        self.outgoing_status = "not started"
        self.input_status = "not started"

        self.reconnect_task: Optional[asyncio.Task] = None
        self.outgoing_monitor_task: Optional[asyncio.Task] = None
        self.http_pull_task: Optional[asyncio.Task] = None
        self.client_writers: Set[asyncio.StreamWriter] = set()

        self.send_lock = asyncio.Lock()
        self.stopping = False
        self.parsed_count = int(record_count)
        self.parse_error_count = 0
        self.sent_count = 0
        self.record_count = self.parsed_count
        self.http_url_counts = {
            url: {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0}
            for url in self.config.http_urls
        }
        self.http_pull_statuses = {
            url: self.make_pending_http_pull_status(url)
            for url in self.config.http_urls
        }

    def make_pending_http_pull_status(self, raw_url: str) -> dict:
        counts = self.http_url_counts.get(
            raw_url,
            {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0},
        )
        return {
            "url": raw_url,
            "effective_url": self.http_pull_url(raw_url),
            "state": "pending",
            "message": "not pulled yet",
            "records": "0",
            "parsed_count": str(counts["parsed_count"]),
            "parse_error_count": str(counts["parse_error_count"]),
            "sent_count": str(counts["sent_count"]),
            "last_pull_at": "",
        }

    def make_http_pull_status(
        self,
        raw_url: str,
        state: str,
        message: str,
        *,
        records: int = 0,
    ) -> dict:
        return {
            "url": raw_url,
            "effective_url": self.http_pull_url(raw_url),
            "state": state,
            "message": message,
            "records": str(records),
            "parsed_count": str(self.http_url_counts.get(raw_url, {}).get("parsed_count", 0)),
            "parse_error_count": str(
                self.http_url_counts.get(raw_url, {}).get("parse_error_count", 0)
            ),
            "sent_count": str(self.http_url_counts.get(raw_url, {}).get("sent_count", 0)),
            "last_pull_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

    def http_pull_status_with_counts(self, raw_url: str) -> dict:
        status = dict(self.http_pull_statuses.get(raw_url, self.make_pending_http_pull_status(raw_url)))
        counts = self.http_url_counts.get(
            raw_url,
            {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0},
        )
        status["parsed_count"] = str(counts["parsed_count"])
        status["parse_error_count"] = str(counts["parse_error_count"])
        status["sent_count"] = str(counts["sent_count"])
        return status

    async def start(self) -> None:
        if (
            self.server is not None
            or self.udp_transport is not None
            or self.http_pull_task is not None
        ):
            return

        self.stopping = False
        self.reconnect_task = asyncio.create_task(self.reconnect_loop())

        if self.config.input_mode == "http_pull":
            self.input_status = (
                f"pulling {len(self.config.http_urls)} URLs "
                f"every {self.config.http_interval_seconds}s"
            )
            self.http_pull_task = asyncio.create_task(self.http_pull_loop())
            print(
                f"Pipe '{self.config.name}' pulling {len(self.config.http_urls)} URLs "
                f"every {self.config.http_interval_seconds}s"
            )
        elif self.config.input_mode == "udp":
            loop = asyncio.get_running_loop()
            transport, _ = await loop.create_datagram_endpoint(
                lambda: UdpPipeProtocol(self),
                local_addr=(self.config.listen_host, self.config.listen_port),
            )
            self.udp_transport = transport
            socket_name = transport.get_extra_info("sockname")
            self.input_status = f"listening for UDP on {socket_name}"
            print(f"Pipe '{self.config.name}' listening for UDP on {socket_name}")
        else:
            self.server = await asyncio.start_server(
                self.handle_client,
                self.config.listen_host,
                self.config.listen_port,
            )
            sockets = ", ".join(str(sock.getsockname()) for sock in self.server.sockets or [])
            self.input_status = f"listening on {sockets}"
            print(f"Pipe '{self.config.name}' listening on {sockets}")

    async def stop(self) -> None:
        self.stopping = True

        if self.http_pull_task is not None:
            self.http_pull_task.cancel()
            try:
                await self.http_pull_task
            except asyncio.CancelledError:
                pass
            self.http_pull_task = None

        if self.reconnect_task is not None:
            self.reconnect_task.cancel()
            try:
                await self.reconnect_task
            except asyncio.CancelledError:
                pass
            self.reconnect_task = None

        await self.close_outgoing()

        if self.udp_transport is not None:
            self.udp_transport.close()
            self.udp_transport = None

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        client_writers = list(self.client_writers)
        for writer in client_writers:
            writer.close()

        for writer in client_writers:
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

        self.client_writers.clear()

        self.outgoing_status = "stopped"
        self.input_status = "stopped"
        print(f"Pipe '{self.config.name}' stopped")

    def http_pull_url(self, http_url: str) -> str:
        parsed = urlsplit(http_url)
        if not self.config.http_port:
            return http_url

        hostname = parsed.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"

        username = parsed.username or ""
        password = f":{parsed.password}" if parsed.password else ""
        auth = f"{username}{password}@" if username else ""
        netloc = f"{auth}{hostname}:{self.config.http_port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))

    async def http_pull_loop(self) -> None:
        timeout = ClientTimeout(total=self.config.http_timeout_seconds)

        async with ClientSession(timeout=timeout) as session:
            while not self.stopping:
                cycle_processed_count = 0
                successful_pulls = 0

                for pull_order, raw_url in enumerate(self.config.http_urls, start=1):
                    url = self.http_pull_url(raw_url)
                    self.http_pull_statuses[raw_url] = self.make_http_pull_status(
                        raw_url,
                        "pulling",
                        "pulling now",
                    )

                    try:
                        async with session.get(url) as response:
                            response.raise_for_status()
                            text = await response.text()

                        processed_count = await self.process_records_text(
                            text,
                            pull_order=pull_order,
                            raw_url=raw_url,
                        )
                        cycle_processed_count += processed_count
                        successful_pulls += 1
                        self.input_status = (
                            f"last pull ok from {url}; processed {processed_count} records"
                        )
                        self.http_pull_statuses[raw_url] = self.make_http_pull_status(
                            raw_url,
                            "ok",
                            f"HTTP {response.status}; processed {processed_count} records",
                            records=processed_count,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.input_status = f"http pull failed for {url}: {exc}"
                        self.http_pull_statuses[raw_url] = self.make_http_pull_status(
                            raw_url,
                            "error",
                            str(exc),
                        )
                        print(f"Pipe '{self.config.name}' HTTP pull failed for {url}: {exc}")

                if successful_pulls:
                    self.input_status = (
                        f"last poll ok for {successful_pulls}/{len(self.config.http_urls)} URLs; "
                        f"processed {cycle_processed_count} records"
                    )

                try:
                    await asyncio.sleep(self.config.http_interval_seconds)
                except asyncio.CancelledError:
                    raise

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        print(f"Pipe '{self.config.name}' accepted connection from {peer}")
        self.client_writers.add(writer)

        buffer = ""
        record_delimiter = self.config.incoming_record_delimiter

        try:
            while not reader.at_eof():
                chunk = await reader.read(4096)
                if not chunk:
                    break

                buffer += chunk.decode("utf-8", errors="replace")

                while record_delimiter in buffer:
                    record, buffer = buffer.split(record_delimiter, 1)
                    await self.process_record(record)
        finally:
            self.client_writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            print(f"Pipe '{self.config.name}' closed connection from {peer}")

    async def handle_udp_datagram(self, data: bytes, addr: object) -> None:
        text = data.decode("utf-8", errors="replace")
        processed_count = await self.process_datagram_text(text)
        self.input_status = f"last UDP datagram from {addr}; processed {processed_count} records"

    async def process_datagram_text(self, text: str) -> int:
        record_delimiter = self.config.incoming_record_delimiter
        if record_delimiter not in text:
            return 1 if await self.process_record(text) else 0

        return await self.process_records_text(text)

    async def process_records_text(
        self,
        text: str,
        *,
        pull_order: int = 0,
        raw_url: str = "",
    ) -> int:
        processed_count = 0
        for record in text.split(self.config.incoming_record_delimiter):
            if await self.process_record(record, pull_order=pull_order, raw_url=raw_url):
                processed_count += 1
        return processed_count

    async def process_record(
        self,
        record: str,
        *,
        pull_order: int = 0,
        raw_url: str = "",
    ) -> bool:
        if not record:
            return False

        try:
            json_record = self.record_to_json(record, pull_order=pull_order)
        except Exception as exc:
            self.parse_error_count += 1
            if raw_url:
                self.http_url_counts.setdefault(
                    raw_url,
                    {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0},
                )["parse_error_count"] += 1
            print(f"Pipe '{self.config.name}' failed record {record!r}: {exc}")
            return False

        self.parsed_count += 1
        self.record_count = self.parsed_count
        if raw_url:
            self.http_url_counts.setdefault(
                raw_url,
                {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0},
            )["parsed_count"] += 1

        try:
            await self.send_outgoing(json_record)
            self.sent_count += 1
            if raw_url:
                self.http_url_counts.setdefault(
                    raw_url,
                    {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0},
                )["sent_count"] += 1
        except Exception as exc:
            print(f"Pipe '{self.config.name}' failed to send record {record!r}: {exc}")

        return True

    async def reconnect_loop(self) -> None:
        while not self.stopping:
            if self.outgoing_writer is None or self.outgoing_writer.is_closing():
                await self.connect_outgoing()

            try:
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)
            except asyncio.CancelledError:
                raise

    async def connect_outgoing(self) -> None:
        await self.close_outgoing()

        self.outgoing_connected = False
        self.outgoing_status = f"connecting to {self.config.outgoing_host}:{self.config.outgoing_port}"

        try:
            self.outgoing_reader, self.outgoing_writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.outgoing_host,
                    self.config.outgoing_port,
                ),
                timeout=CONNECT_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.outgoing_reader = None
            self.outgoing_writer = None
            self.outgoing_connected = False
            self.outgoing_status = f"endpoint disconnected: {exc}"
            return

        self.outgoing_connected = True
        self.outgoing_status = f"connected to {self.config.outgoing_host}:{self.config.outgoing_port}"

        if self.outgoing_monitor_task is not None:
            self.outgoing_monitor_task.cancel()

        self.outgoing_monitor_task = asyncio.create_task(self.monitor_outgoing())
        print(
            f"Pipe '{self.config.name}' connected to endpoint "
            f"{self.config.outgoing_host}:{self.config.outgoing_port}"
        )

    async def monitor_outgoing(self) -> None:
        try:
            if self.outgoing_reader is None:
                return

            await self.outgoing_reader.read()

            if not self.stopping:
                self.outgoing_connected = False
                self.outgoing_status = "endpoint disconnected"
                await self.close_outgoing()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.stopping:
                self.outgoing_connected = False
                self.outgoing_status = f"endpoint disconnected: {exc}"
                await self.close_outgoing()

    async def close_outgoing(self) -> None:
        current_task = asyncio.current_task()

        if (
            self.outgoing_monitor_task is not None
            and self.outgoing_monitor_task is not current_task
        ):
            self.outgoing_monitor_task.cancel()
            try:
                await self.outgoing_monitor_task
            except asyncio.CancelledError:
                pass
            self.outgoing_monitor_task = None

        if self.outgoing_writer is not None:
            self.outgoing_writer.close()
            try:
                await asyncio.wait_for(
                    self.outgoing_writer.wait_closed(),
                    timeout=CLOSE_TIMEOUT_SECONDS,
                )
            except Exception:
                pass

        self.outgoing_reader = None
        self.outgoing_writer = None
        self.outgoing_connected = False

    def status(self) -> dict:
        return {
            "outgoing_connected": self.outgoing_connected,
            "outgoing_status": self.outgoing_status,
            "input_status": self.input_status,
            "http_pull_statuses": [
                self.http_pull_status_with_counts(url)
                for url in self.config.http_urls
            ],
            "record_count": str(self.parsed_count),
            "parsed_count": str(self.parsed_count),
            "parse_error_count": str(self.parse_error_count),
            "sent_count": str(self.sent_count),
        }

    def record_to_json(self, record: str, *, pull_order: int = 0) -> str:
        raw_fields = self.split_record(record)
        fields = list(raw_fields)
        included_indexes = parse_included_fields(self.config.included_fields)
        if included_indexes:
            fields = [
                fields[index]
                for index in included_indexes
                if index < len(fields)
            ]
        strip_set = "".join(self.config.strip_chars)

        obj = {}
        for index, value in enumerate(fields):
            if strip_set:
                value = value.strip(strip_set)

            if index < len(self.config.field_names):
                key = self.config.field_names[index]
                field_type = self.config.field_types[index]
            else:
                key = f"#undef-{index + 1}"
                field_type = "text"

            if field_type == "numeric":
                obj[key] = self.parse_numeric(value, key)
            else:
                obj[key] = value

        for tag in self.config.json_tags:
            obj[tag["name"]] = self.evaluate_json_tag(tag, raw_fields, pull_order=pull_order)

        return json.dumps(obj, separators=(",", ":")) + self.config.outgoing_record_delimiter

    def evaluate_json_tag(self, tag: dict, fields: List[str], *, pull_order: int = 0) -> object:
        source = tag.get("source", "constant")

        if source == "constant":
            value = self.render_special_tokens(str(tag.get("value", "")), pull_order=pull_order)
            if tag.get("value_type") == "numeric":
                return self.parse_numeric(value, tag["name"])
            return value

        if source == "field":
            field_number = int(tag.get("field_number") or 0)
            value = self.get_field_by_number(fields, field_number)
            if tag.get("value_type") == "numeric":
                return self.parse_numeric(value, tag["name"])
            return self.apply_text_transform(value, tag.get("transform", "none"))

        if source == "numeric":
            return self.evaluate_numeric_expression(
                str(tag.get("value", "")),
                fields,
                pull_order=pull_order,
            )

        if source == "text":
            value = self.render_text_template(
                str(tag.get("value", "")),
                fields,
                pull_order=pull_order,
            )
            return self.apply_text_transform(value, tag.get("transform", "none"))

        raise ValueError(f"Extra JSON tag '{tag.get('name', '')}' has an invalid source.")

    @staticmethod
    def get_field_by_number(fields: List[str], field_number: int) -> str:
        if field_number < 1 or field_number > len(fields):
            raise ValueError(f"CSV field {field_number} is not present in the input record.")
        return fields[field_number - 1]

    def render_text_template(self, template: str, fields: List[str], *, pull_order: int = 0) -> str:
        def replace_field(match: re.Match) -> str:
            field_number = int(match.group(1))
            return self.get_field_by_number(fields, field_number)

        rendered = re.sub(r"\{(\d+)\}", replace_field, template)
        return self.render_special_tokens(rendered, pull_order=pull_order)

    @staticmethod
    def render_special_tokens(value: str, *, pull_order: int = 0) -> str:
        def replace_order_list(match: re.Match) -> str:
            try:
                items = json.loads(match.group(1))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid #[] ordered substring list: {exc}") from exc

            if not isinstance(items, list):
                raise ValueError("#[] ordered substring token must contain a JSON array.")

            index = pull_order - 1
            if index < 0 or index >= len(items):
                raise ValueError(
                    f"#[] ordered substring list has no value for pull order {pull_order}."
                )

            return str(items[index])

        value = re.sub(r"#(\[[^\r\n]*?\])", replace_order_list, value)
        return value.replace("#order", str(pull_order))

    @staticmethod
    def apply_text_transform(value: str, transform: str) -> str:
        if transform == "strip":
            return value.strip()
        if transform == "upper":
            return value.upper()
        if transform == "lower":
            return value.lower()
        if transform == "title":
            return value.title()
        if transform == "lstrip":
            return value.lstrip()
        if transform == "rstrip":
            return value.rstrip()
        return value

    def evaluate_numeric_expression(
        self,
        expression: str,
        fields: List[str],
        *,
        pull_order: int = 0,
    ) -> int | float:
        if not expression.strip():
            raise ValueError("Numeric extra JSON tag expression cannot be blank.")

        tree = ast.parse(expression, mode="eval")
        result = self.evaluate_numeric_node(tree.body, fields, pull_order=pull_order)

        if isinstance(result, float) and result.is_integer():
            return int(result)
        return result

    def evaluate_numeric_node(
        self,
        node: ast.AST,
        fields: List[str],
        *,
        pull_order: int = 0,
    ) -> int | float:
        if isinstance(node, ast.BinOp) and type(node.op) in NUMERIC_OPERATORS:
            left = self.evaluate_numeric_node(node.left, fields, pull_order=pull_order)
            right = self.evaluate_numeric_node(node.right, fields, pull_order=pull_order)
            return NUMERIC_OPERATORS[type(node.op)](left, right)

        if isinstance(node, ast.UnaryOp) and type(node.op) in NUMERIC_UNARY_OPERATORS:
            value = self.evaluate_numeric_node(node.operand, fields, pull_order=pull_order)
            return NUMERIC_UNARY_OPERATORS[type(node.op)](value)

        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value

        if isinstance(node, ast.Name) and re.fullmatch(r"f\d+", node.id):
            field_number = int(node.id[1:])
            return self.parse_numeric(self.get_field_by_number(fields, field_number), node.id)

        if isinstance(node, ast.Name) and node.id == "order":
            return pull_order

        raise ValueError(
            "Numeric expressions may only use numbers, f1-style CSV field references, "
            "order, parentheses, and + - * / // % operators."
        )

    def split_record(self, record: str) -> List[str]:
        """
        Split one record using one or more delimiters.

        Double-quoted text is supported. Delimiters inside quoted text are
        preserved. Doubled quotes inside quoted text become one quote.
        """
        if self.config.delimiter_mode == "regex":
            return self.split_record_regex(record)

        delimiters = sorted(self.config.delimiters, key=len, reverse=True)
        fields: List[str] = []
        current: List[str] = []
        index = 0
        in_quotes = False

        while index < len(record):
            char = record[index]

            if char == '"':
                if in_quotes and index + 1 < len(record) and record[index + 1] == '"':
                    current.append('"')
                    index += 2
                    continue

                in_quotes = not in_quotes
                index += 1
                continue

            if not in_quotes:
                matched_delimiter = None
                for delimiter in delimiters:
                    if record.startswith(delimiter, index):
                        matched_delimiter = delimiter
                        break

                if matched_delimiter is not None:
                    fields.append("".join(current))
                    current = []
                    index += len(matched_delimiter)
                    continue

            current.append(char)
            index += 1

        fields.append("".join(current))
        return fields

    def split_record_regex(self, record: str) -> List[str]:
        delimiters = [re.compile(pattern) for pattern in self.config.delimiters]
        fields: List[str] = []
        current: List[str] = []
        index = 0
        in_quotes = False

        while index < len(record):
            char = record[index]

            if char == '"':
                if in_quotes and index + 1 < len(record) and record[index + 1] == '"':
                    current.append('"')
                    index += 2
                    continue

                in_quotes = not in_quotes
                index += 1
                continue

            if not in_quotes:
                matched_delimiter = None
                for delimiter in delimiters:
                    match = delimiter.match(record, index)
                    if match is not None and match.end() > match.start():
                        matched_delimiter = match
                        break

                if matched_delimiter is not None:
                    fields.append("".join(current))
                    current = []
                    index = matched_delimiter.end()
                    continue

            current.append(char)
            index += 1

        fields.append("".join(current))
        return fields

    @staticmethod
    def parse_numeric(value: str, key: str) -> int | float:
        value = value.strip()
        if value == "":
            raise ValueError(f"Numeric field '{key}' is blank.")

        try:
            number = float(value)
        except ValueError as exc:
            raise ValueError(f"Numeric field '{key}' has invalid value {value!r}.") from exc

        if number.is_integer() and "." not in value and "e" not in value.lower():
            return int(value)
        return number

    async def send_outgoing(self, json_record: str) -> None:
        async with self.send_lock:
            if self.outgoing_writer is None or self.outgoing_writer.is_closing():
                await self.connect_outgoing()

            if self.outgoing_writer is None:
                raise ConnectionError(self.outgoing_status)

            try:
                self.outgoing_writer.write(json_record.encode("utf-8"))
                await self.outgoing_writer.drain()
                self.outgoing_connected = True
                self.outgoing_status = (
                    f"connected to {self.config.outgoing_host}:{self.config.outgoing_port}"
                )
            except Exception as exc:
                self.outgoing_connected = False
                self.outgoing_status = f"endpoint disconnected: {exc}"
                await self.close_outgoing()
                raise


class UdpPipeProtocol(asyncio.DatagramProtocol):
    def __init__(self, runtime: PipeRuntime):
        self.runtime = runtime

    def datagram_received(self, data: bytes, addr: object) -> None:
        asyncio.create_task(self.runtime.handle_udp_datagram(data, addr))

    def error_received(self, exc: Exception) -> None:
        self.runtime.input_status = f"UDP receiver error: {exc}"
        print(f"Pipe '{self.runtime.config.name}' UDP receiver error: {exc}")


class PipeManager:
    def __init__(self, store: ConfigStore):
        self.store = store
        self.runtimes: Dict[str, PipeRuntime] = {}
        self.record_counts: Dict[str, int] = {}

    @staticmethod
    def is_port_available(host: str, port: int, protocol: str = "tcp") -> bool:
        socket_type = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
        try:
            with socket.socket(socket.AF_INET, socket_type) as test_socket:
                if protocol == "tcp":
                    test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                test_socket.bind((host, int(port)))
            return True
        except OSError:
            return False

    def validate_pipe_assignment(self, pipe: PipeConfig, original_name: str = "") -> None:
        pipe.validate()

        for existing_name, existing_pipe in self.store.pipes.items():
            if existing_name == original_name or existing_name == pipe.name:
                continue

            if (
                pipe.input_mode in {"tcp", "udp"}
                and existing_pipe.input_mode == pipe.input_mode
                and int(existing_pipe.listen_port) == int(pipe.listen_port)
            ):
                raise ValueError(
                    f"Incoming port {pipe.listen_port} is already assigned "
                    f"to pipe '{existing_name}'."
                )

        active_same_pipe = self.runtimes.get(original_name or pipe.name)
        same_active_port = (
            active_same_pipe is not None
            and pipe.input_mode in {"tcp", "udp"}
            and active_same_pipe.config.input_mode == pipe.input_mode
            and int(active_same_pipe.config.listen_port) == int(pipe.listen_port)
            and active_same_pipe.config.listen_host == pipe.listen_host
        )

        if pipe.enabled and pipe.input_mode in {"tcp", "udp"} and not same_active_port:
            if not self.is_port_available(pipe.listen_host, pipe.listen_port, pipe.input_mode):
                raise ValueError(
                    f"Incoming port {pipe.listen_port} on {pipe.listen_host} "
                    f"is already in use."
                )

    async def sync(self) -> None:
        desired_names = set(self.store.pipes.keys())
        active_names = set(self.runtimes.keys())

        for name in active_names - desired_names:
            self.record_counts[name] = self.runtimes[name].record_count
            await self.runtimes[name].stop()
            del self.runtimes[name]

        for name in desired_names:
            self.record_counts.setdefault(name, 0)

        for name in set(self.record_counts.keys()) - desired_names - active_names:
            del self.record_counts[name]

        for name, config in self.store.pipes.items():
            existing = self.runtimes.get(name)

            if existing is not None and asdict(existing.config) != asdict(config):
                self.record_counts[name] = 0 if not config.enabled else existing.record_count
                await existing.stop()
                del self.runtimes[name]
                existing = None

            if config.enabled and existing is None:
                runtime = PipeRuntime(config, self.record_counts.get(name, 0))
                await runtime.start()
                self.runtimes[name] = runtime

            if not config.enabled and existing is not None:
                self.record_counts[name] = 0
                await existing.stop()
                del self.runtimes[name]

        for name in set(self.record_counts.keys()) - desired_names:
            del self.record_counts[name]

    def get_status(self, name: str) -> dict:
        runtime = self.runtimes.get(name)
        if runtime is None:
            return {
                "outgoing_connected": False,
                "outgoing_status": "disabled or not running",
                "input_status": "disabled or not running",
                "http_pull_statuses": [],
                "record_count": str(self.record_counts.get(name, 0)),
                "parsed_count": str(self.record_counts.get(name, 0)),
                "parse_error_count": "0",
                "sent_count": "0",
            }
        return runtime.status()

    def reset_record_count(self, name: str) -> None:
        if name not in self.store.pipes:
            raise KeyError(name)

        self.record_counts[name] = 0
        runtime = self.runtimes.get(name)
        if runtime is not None:
            runtime.parsed_count = 0
            runtime.parse_error_count = 0
            runtime.sent_count = 0
            runtime.record_count = 0
            runtime.http_url_counts = {
                url: {"parsed_count": 0, "parse_error_count": 0, "sent_count": 0}
                for url in runtime.config.http_urls
            }
            runtime.http_pull_statuses = {
                url: runtime.make_pending_http_pull_status(url)
                for url in runtime.config.http_urls
            }

    async def stop_all(self) -> None:
        for runtime in list(self.runtimes.values()):
            self.record_counts[runtime.config.name] = runtime.record_count
            await runtime.stop()
        self.runtimes.clear()


store = ConfigStore(CONFIG_FILE)
manager = PipeManager(store)


HTML_PAGE = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Data Pipe Configuration</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 1rem; background: #f7f7f7; line-height: 1.3; }
    h1 { margin: 0 0 .2rem; }
    h2, h3, p { margin: .35rem 0; }
    .card { background: white; border: 1px solid #ddd; border-radius: 8px; padding: .7rem; margin: .7rem 0; box-shadow: 0 1px 4px #ddd; }
    .pipe-card { display: grid; grid-template-columns: minmax(12rem, 1fr) 8.5rem minmax(18rem, 24rem) auto; align-items: center; gap: .75rem; }
    .pipe-card h3 { margin: 0; }
    .pipe-actions { display: flex; justify-content: flex-end; gap: .35rem; flex-wrap: wrap; }
    .pipe-actions button { margin-top: 0; }
    .pipe-records { min-width: 0; overflow-wrap: anywhere; }
    .counter-grid { display: grid; grid-template-columns: repeat(3, minmax(4.8rem, 1fr)); gap: .35rem; min-width: 0; }
    .counter-field { border: 1px solid #ddd; border-radius: 6px; padding: .24rem .4rem; background: #fafafa; min-width: 0; }
    .counter-label { display: block; color: #666; font-size: .72rem; font-weight: 700; text-transform: uppercase; letter-spacing: .02em; white-space: nowrap; }
    .record-count { display: block; font-variant-numeric: tabular-nums; font-weight: 800; line-height: 1.15; }
    .status-badge { display: inline-block; border-radius: 999px; padding: .2rem .55rem; font-size: .85rem; font-weight: 700; }
    .status-connected { background: #e8f5e9; color: #1b5e20; border: 1px solid #a5d6a7; }
    .status-disconnected { background: #fff8e1; color: #7a4f00; border: 1px solid #ffcc80; }
    .status-disabled { background: #eeeeee; color: #555; border: 1px solid #ccc; }
    .pull-statuses { grid-column: 1 / -1; display: grid; gap: .25rem; font-size: .9rem; }
    .pull-status-row { display: grid; grid-template-columns: 4.8rem minmax(12rem, 1fr) minmax(15rem, 22rem) minmax(10rem, 1.4fr) 9rem; gap: .45rem; align-items: center; min-width: 0; }
    .pull-state { border-radius: 999px; padding: .12rem .45rem; font-size: .78rem; font-weight: 700; text-align: center; }
    .pull-state-ok { background: #e8f5e9; color: #1b5e20; border: 1px solid #a5d6a7; }
    .pull-state-error { background: #ffebee; color: #8a1111; border: 1px solid #ef9a9a; }
    .pull-state-pulling { background: #e3f2fd; color: #0d47a1; border: 1px solid #90caf9; }
    .pull-state-pending { background: #eeeeee; color: #555; border: 1px solid #ccc; }
    .pull-url, .pull-message, .pull-time { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    label { display: block; margin-top: .35rem; font-weight: 600; }
    input, select, textarea { width: 100%; box-sizing: border-box; padding: .28rem .4rem; margin-top: .1rem; }
    textarea { min-height: 2.35rem; font-family: monospace; }
    button { margin-top: .45rem; margin-right: .35rem; padding: .4rem .7rem; border: 0; border-radius: 6px; cursor: pointer; }
    button:disabled { opacity: .6; cursor: not-allowed; }
    table { width: 100%; border-collapse: collapse; margin-top: .3rem; }
    th, td { border-bottom: 1px solid #ddd; padding: .18rem .3rem; text-align: left; vertical-align: top; }
    td button { margin-top: 0; }
    .primary { background: #1f6feb; color: white; }
    .danger { background: #c62828; color: white; }
    .muted { color: #666; }
    .notice { display: none; padding: .5rem .7rem; border-radius: 6px; margin: .6rem 0; }
    .notice.ok { display: block; background: #e8f5e9; border: 1px solid #a5d6a7; }
    .notice.error { display: block; background: #ffebee; border: 1px solid #ef9a9a; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: .65rem; }
    .definition-grid { display: grid; grid-template-columns: repeat(4, minmax(9rem, 1fr)); gap: .35rem .65rem; }
    .span-2 { grid-column: span 2; }
    .compact-help { font-size: .9rem; margin: .18rem 0 .28rem; }
    .field-header { display: flex; align-items: center; justify-content: space-between; gap: .6rem; margin-top: .45rem; }
    .field-header h3 { margin: 0; }
    .field-header button { margin-top: 0; }
    .form-card-header { display: flex; align-items: center; justify-content: space-between; gap: .75rem; }
    .form-card-header h2 { margin: 0; }
    .form-card-header button { margin-top: 0; margin-right: 0; }
    .form-panel { margin-top: .55rem; }
    .form-footer { display: flex; align-items: center; gap: .6rem; flex-wrap: wrap; margin-top: .45rem; }
    .form-footer button { margin-top: 0; }
    .enabled-toggle { display: inline-flex; align-items: center; gap: .3rem; margin-top: 0; }
    .enabled-toggle input { width: auto; margin-top: 0; }
    .hidden { display: none !important; }
    @media (max-width: 850px) {
      .definition-grid { grid-template-columns: 1fr 1fr; }
      .span-2 { grid-column: span 2; }
    }
    @media (max-width: 560px) {
      .definition-grid, .row, .pipe-card { grid-template-columns: 1fr; }
      .pull-status-row { grid-template-columns: 1fr; gap: .12rem; }
      .counter-grid { grid-template-columns: repeat(3, minmax(4.6rem, 1fr)); }
      .span-2 { grid-column: span 1; }
      .pipe-actions { justify-content: flex-start; }
    }
    code { background: #eee; padding: .15rem .3rem; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Data Pipe Configuration</h1>
  <p class="muted">Records are received over TCP/UDP or pulled from HTTP and transformed into delimited JSON records.</p>
  <div id="notice" class="notice"></div>

  <div class="card">
    <div class="form-card-header">
      <h2 id="form-title">Add Pipe</h2>
      <button id="toggle-form-button" type="button" aria-controls="pipe-form-panel" aria-expanded="false">Show Form</button>
    </div>
    <div id="pipe-form-panel" class="form-panel hidden">
    <form id="pipe-form">
      <input type="hidden" id="original_name">

      <div class="definition-grid">
        <div class="span-2">
          <label for="name">Name</label>
          <input id="name" required placeholder="orders_pipe">
        </div>
        <div class="span-2">
          <label for="input_mode">Input Mode</label>
          <select id="input_mode">
            <option value="tcp">TCP Push</option>
            <option value="udp">UDP Push</option>
            <option value="http_pull">HTTP Pull</option>
          </select>
        </div>
        <div class="tcp-input">
          <label for="listen_host">Listen Host</label>
          <input id="listen_host" value="0.0.0.0">
        </div>
        <div class="tcp-input">
          <label for="listen_port">Listen Port</label>
          <input id="listen_port" type="number" min="1" max="65535" required value="9000">
        </div>
        <div class="span-2 http-input hidden">
          <label for="http_urls">External HTTP URLs</label>
          <textarea id="http_urls" placeholder="One URL per line. Example:
https://example.com/data-a.csv
https://example.com/data-b.csv"></textarea>
        </div>
        <div class="http-input hidden">
          <label for="http_port">External HTTP Port for All URLs</label>
          <input id="http_port" type="number" min="0" max="65535" value="0">
        </div>
        <div class="http-input hidden">
          <label for="http_interval_seconds">Pull Period Seconds</label>
          <input id="http_interval_seconds" type="number" min="1" value="60">
        </div>
        <div class="http-input hidden">
          <label for="http_timeout_seconds">HTTP Timeout Seconds</label>
          <input id="http_timeout_seconds" type="number" min="1" value="10">
        </div>
        <div>
          <label for="outgoing_host">Outgoing Host</label>
          <input id="outgoing_host" required value="127.0.0.1">
        </div>
        <div>
          <label for="outgoing_port">Outgoing Port</label>
          <input id="outgoing_port" type="number" min="1" max="65535" required value="9100">
        </div>
        <div>
          <label for="incoming_record_delimiter">Incoming Record Delimiter</label>
          <input id="incoming_record_delimiter" value="\n">
        </div>
        <div>
          <label for="outgoing_record_delimiter">Outgoing Record Delimiter</label>
          <input id="outgoing_record_delimiter" value="\n">
        </div>
        <div class="span-2">
          <label for="delimiters">Field Delimiters</label>
          <textarea id="delimiters" placeholder="One delimiter per line. Examples:
,
|
\t
\x1e
\s+">,</textarea>
        </div>
        <div>
          <label for="delimiter_mode">Delimiter Mode</label>
          <select id="delimiter_mode">
            <option value="literal">Literal</option>
            <option value="regex">Regex</option>
          </select>
        </div>
        <div class="span-2">
          <label for="strip_chars">Characters to Strip From JSON Values</label>
          <textarea id="strip_chars" placeholder="One per line. Examples:
space
\t
\r
\n"></textarea>
        </div>
        <div class="span-2">
          <label for="included_fields">Included CSV Fields</label>
          <input id="included_fields" placeholder="Blank for all fields. Examples: 1-3 or 1-3,4,6,7">
        </div>
      </div>
      <p class="muted compact-help">Literal delimiter escapes: <code>\n</code>, <code>\r</code>, <code>\t</code>, <code>\x1e</code>. Regex delimiter example: <code>\s+</code> for one or more spaces. Use <code>space</code> in strip chars for a space. Included CSV fields are 1-based and are applied before JSON field names.</p>

      <div class="field-header">
        <h3>JSON Fields</h3>
        <button id="add-field-button" type="button">Add JSON Field</button>
      </div>
      <table>
        <thead><tr><th>JSON field name</th><th>Type</th><th></th></tr></thead>
        <tbody id="fields-body"></tbody>
      </table>

      <div class="field-header">
        <h3>Extra JSON Tags</h3>
        <button id="add-tag-button" type="button">Add JSON Tag</button>
      </div>
      <table>
        <thead><tr><th>Tag name</th><th>Source</th><th>Field #</th><th>Value / expression / template</th><th>Type</th><th>Text transform</th><th></th></tr></thead>
        <tbody id="tags-body"></tbody>
      </table>
      <p class="muted compact-help">Extra tags use original CSV field numbers before included-field filtering. Numeric expressions can use <code>f1</code>, <code>f2</code>, <code>order</code>, and <code>+ - * / // %</code>. Text or constant values can use <code>#order</code> or ordered lists like <code>src#["a","b","c"]</code>; text templates can also use <code>{1}</code>, <code>{2}</code>.</p>

      <div class="form-footer">
        <label class="enabled-toggle">
          <input id="enabled" type="checkbox" checked>
          Enabled
        </label>
        <button id="save-button" class="primary" type="submit">Save Pipe</button>
        <button id="clear-button" type="button">Clear Form</button>
      </div>
    </form>
    </div>
  </div>

  <div class="card">
    <h2>Existing Pipes</h2>
    <p class="muted">Use this section to edit, enable, disable, or delete configured data pipes.</p>
    <div id="pipes">Loading pipes...</div>
  </div>

<script>
(function () {
  const $ = id => document.getElementById(id);

  function setFormExpanded(expanded) {
    const panel = $('pipe-form-panel');
    const button = $('toggle-form-button');
    panel.classList.toggle('hidden', !expanded);
    button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    button.textContent = expanded ? 'Hide Form' : 'Show Form';
  }

  function showNotice(message, type) {
    const notice = $('notice');
    notice.className = 'notice ' + (type || 'ok');
    notice.textContent = message;
  }

  function clearNotice() {
    const notice = $('notice');
    notice.className = 'notice';
    notice.textContent = '';
  }

  async function api(path, options) {
    const response = await fetch(path, Object.assign({
      headers: { 'Content-Type': 'application/json' }
    }, options || {}));

    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || response.statusText);
    }
    return response.json();
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function linesFromTextarea(id) {
    return $(id).value
      .split(/\r?\n/)
      .map(x => x.trim())
      .filter(x => x.length > 0);
  }

  function formatPullTime(value) {
    if (!value) return 'never';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString();
  }

  function counterGrid(parsed, errors, sent) {
    return `
      <span class="counter-grid">
        <span class="counter-field"><span class="counter-label">Parsed</span><span class="record-count">${escapeHtml(parsed || '0')}</span></span>
        <span class="counter-field"><span class="counter-label">Errors</span><span class="record-count">${escapeHtml(errors || '0')}</span></span>
        <span class="counter-field"><span class="counter-label">Sent</span><span class="record-count">${escapeHtml(sent || '0')}</span></span>
      </span>
    `;
  }

  function updateInputModeFields() {
    const mode = $('input_mode').value || 'tcp';
    const isHttpPull = mode === 'http_pull';

    document.querySelectorAll('.tcp-input').forEach(element => {
      element.classList.toggle('hidden', isHttpPull);
    });
    document.querySelectorAll('.http-input').forEach(element => {
      element.classList.toggle('hidden', !isHttpPull);
    });

    $('listen_port').required = !isHttpPull;
    $('http_urls').required = isHttpPull;
    $('http_interval_seconds').required = isHttpPull;
    $('http_timeout_seconds').required = isHttpPull;
  }

  function inputModeLabel(mode) {
    if (mode === 'http_pull') return 'HTTP Pull';
    if (mode === 'udp') return 'UDP Push';
    return 'TCP Push';
  }

  function addFieldRow(name, type) {
    const row = document.createElement('tr');

    const nameCell = document.createElement('td');
    const nameInput = document.createElement('input');
    nameInput.className = 'field-name';
    nameInput.placeholder = 'id';
    nameInput.value = name || '';
    nameCell.appendChild(nameInput);

    const typeCell = document.createElement('td');
    const typeSelect = document.createElement('select');
    typeSelect.className = 'field-type';

    const textOption = document.createElement('option');
    textOption.value = 'text';
    textOption.textContent = 'text';

    const numericOption = document.createElement('option');
    numericOption.value = 'numeric';
    numericOption.textContent = 'numeric';

    typeSelect.appendChild(textOption);
    typeSelect.appendChild(numericOption);
    typeSelect.value = type || 'text';
    typeCell.appendChild(typeSelect);

    const actionCell = document.createElement('td');
    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => row.remove());
    actionCell.appendChild(removeButton);

    row.appendChild(nameCell);
    row.appendChild(typeCell);
    row.appendChild(actionCell);

    $('fields-body').appendChild(row);
  }

  function getFields() {
    const names = [];
    const types = [];

    $('fields-body').querySelectorAll('tr').forEach(row => {
      const name = row.querySelector('.field-name').value.trim();
      const type = row.querySelector('.field-type').value;

      if (name) {
        names.push(name);
        types.push(type);
      }
    });

    return { names, types };
  }

  function addTagRow(tag) {
    tag = tag || {};

    const row = document.createElement('tr');

    const nameCell = document.createElement('td');
    const nameInput = document.createElement('input');
    nameInput.className = 'tag-name';
    nameInput.placeholder = 'source_id';
    nameInput.value = tag.name || '';
    nameCell.appendChild(nameInput);

    const sourceCell = document.createElement('td');
    const sourceSelect = document.createElement('select');
    sourceSelect.className = 'tag-source';
    [
      ['constant', 'constant'],
      ['field', 'field'],
      ['numeric', 'numeric expr'],
      ['text', 'text template']
    ].forEach(([value, label]) => {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = label;
      sourceSelect.appendChild(option);
    });
    sourceSelect.value = tag.source || 'constant';
    sourceCell.appendChild(sourceSelect);

    const fieldCell = document.createElement('td');
    const fieldInput = document.createElement('input');
    fieldInput.className = 'tag-field-number';
    fieldInput.type = 'number';
    fieldInput.min = '1';
    fieldInput.placeholder = '1';
    fieldInput.value = tag.field_number || '';
    fieldCell.appendChild(fieldInput);

    const valueCell = document.createElement('td');
    const valueInput = document.createElement('input');
    valueInput.className = 'tag-value';
    valueInput.placeholder = 'constant, f1 * 2, or {1}-{2}';
    valueInput.value = tag.value || '';
    valueCell.appendChild(valueInput);

    const typeCell = document.createElement('td');
    const typeSelect = document.createElement('select');
    typeSelect.className = 'tag-value-type';
    ['text', 'numeric'].forEach(value => {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value;
      typeSelect.appendChild(option);
    });
    typeSelect.value = tag.value_type || 'text';
    typeCell.appendChild(typeSelect);

    const transformCell = document.createElement('td');
    const transformSelect = document.createElement('select');
    transformSelect.className = 'tag-transform';
    ['none', 'strip', 'upper', 'lower', 'title', 'lstrip', 'rstrip'].forEach(value => {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value;
      transformSelect.appendChild(option);
    });
    transformSelect.value = tag.transform || 'none';
    transformCell.appendChild(transformSelect);

    const actionCell = document.createElement('td');
    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => row.remove());
    actionCell.appendChild(removeButton);

    function updateTagControls() {
      const source = sourceSelect.value;
      fieldInput.disabled = source !== 'field';
      valueInput.disabled = source === 'field';
      typeSelect.disabled = source === 'text';
      transformSelect.disabled = !['field', 'text'].includes(source);
    }

    sourceSelect.addEventListener('change', updateTagControls);
    updateTagControls();

    row.appendChild(nameCell);
    row.appendChild(sourceCell);
    row.appendChild(fieldCell);
    row.appendChild(valueCell);
    row.appendChild(typeCell);
    row.appendChild(transformCell);
    row.appendChild(actionCell);

    $('tags-body').appendChild(row);
  }

  function getJsonTags() {
    const tags = [];

    $('tags-body').querySelectorAll('tr').forEach(row => {
      const name = row.querySelector('.tag-name').value.trim();
      const source = row.querySelector('.tag-source').value;
      const fieldNumberValue = row.querySelector('.tag-field-number').value;

      if (!name) {
        return;
      }

      tags.push({
        name: name,
        source: source,
        field_number: Number(fieldNumberValue || 0),
        value: row.querySelector('.tag-value').value,
        value_type: row.querySelector('.tag-value-type').value,
        transform: row.querySelector('.tag-transform').value
      });
    });

    return tags;
  }

  function getFormPipe() {
    const fields = getFields();
    const delimiters = linesFromTextarea('delimiters');

    return {
      name: $('name').value.trim(),
      input_mode: $('input_mode').value || 'tcp',
      listen_host: $('listen_host').value.trim() || '0.0.0.0',
      listen_port: Number($('listen_port').value),
      outgoing_host: $('outgoing_host').value.trim(),
      outgoing_port: Number($('outgoing_port').value),
      http_url: linesFromTextarea('http_urls')[0] || '',
      http_urls: linesFromTextarea('http_urls'),
      http_port: Number($('http_port').value || 0),
      http_interval_seconds: Number($('http_interval_seconds').value || 60),
      http_timeout_seconds: Number($('http_timeout_seconds').value || 10),
      incoming_record_delimiter: $('incoming_record_delimiter').value || '\\n',
      outgoing_record_delimiter: $('outgoing_record_delimiter').value || '\\n',
      delimiter: delimiters[0] || ',',
      delimiter_mode: $('delimiter_mode').value || 'literal',
      delimiters: delimiters,
      strip_chars: linesFromTextarea('strip_chars'),
      included_fields: $('included_fields').value.trim(),
      field_names: fields.names,
      field_types: fields.types,
      json_tags: getJsonTags(),
      enabled: $('enabled').checked
    };
  }

  function setFormPipe(pipe) {
    $('form-title').textContent = 'Edit Pipe';
    setFormExpanded(true);
    $('original_name').value = pipe.name;
    $('name').value = pipe.name;
    $('input_mode').value = pipe.input_mode || 'tcp';
    $('listen_host').value = pipe.listen_host;
    $('listen_port').value = pipe.listen_port;
    $('outgoing_host').value = pipe.outgoing_host;
    $('outgoing_port').value = pipe.outgoing_port;
    $('http_urls').value = (pipe.http_urls && pipe.http_urls.length ? pipe.http_urls : (pipe.http_url ? [pipe.http_url] : [])).join('\n');
    $('http_port').value = pipe.http_port || 0;
    $('http_interval_seconds').value = pipe.http_interval_seconds || 60;
    $('http_timeout_seconds').value = pipe.http_timeout_seconds || 10;
    $('incoming_record_delimiter').value = pipe.incoming_record_delimiter || '\\n';
    $('outgoing_record_delimiter').value = pipe.outgoing_record_delimiter || '\\n';
    $('delimiter_mode').value = pipe.delimiter_mode || 'literal';
    $('delimiters').value = (pipe.delimiters && pipe.delimiters.length ? pipe.delimiters : [pipe.delimiter || ',']).join('\n');
    $('strip_chars').value = (pipe.strip_chars && pipe.strip_chars.length ? pipe.strip_chars : []).join('\n');
    $('included_fields').value = pipe.included_fields || '';
    $('enabled').checked = Boolean(pipe.enabled);

    $('fields-body').innerHTML = '';
    (pipe.field_names || []).forEach((fieldName, index) => {
      const fieldType = (pipe.field_types || [])[index] || 'text';
      addFieldRow(fieldName, fieldType);
    });

    if ((pipe.field_names || []).length === 0) {
      addFieldRow('', 'text');
    }

    $('tags-body').innerHTML = '';
    (pipe.json_tags || []).forEach(tag => addTagRow(tag));

    updateInputModeFields();
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function resetForm() {
    $('form-title').textContent = 'Add Pipe';
    $('original_name').value = '';
    $('pipe-form').reset();
    $('input_mode').value = 'tcp';
    $('listen_host').value = '0.0.0.0';
    $('listen_port').value = '9000';
    $('outgoing_host').value = '127.0.0.1';
    $('outgoing_port').value = '9100';
    $('http_urls').value = '';
    $('http_port').value = '0';
    $('http_interval_seconds').value = '60';
    $('http_timeout_seconds').value = '10';
    $('incoming_record_delimiter').value = '\\n';
    $('outgoing_record_delimiter').value = '\\n';
    $('delimiter_mode').value = 'literal';
    $('delimiters').value = ',';
    $('strip_chars').value = '';
    $('included_fields').value = '';
    $('enabled').checked = true;
    $('fields-body').innerHTML = '';
    addFieldRow('', 'text');
    $('tags-body').innerHTML = '';
    updateInputModeFields();
    setFormExpanded(false);
  }

  async function loadPipes() {
    const data = await api('/api/pipes?cacheBust=' + Date.now());
    const pipes = data.pipes || [];
    const container = $('pipes');

    if (pipes.length === 0) {
      container.innerHTML = '<p>No pipes configured yet. Use the form above to create the first pipe.</p>';
      return;
    }

    container.innerHTML = '';

    pipes.forEach(pipe => {
      const card = document.createElement('div');
      card.className = 'card pipe-card';
      const statusLabel = !pipe.enabled
        ? 'Disabled'
        : (pipe.outgoing_connected ? 'Connected' : 'Disconnected');
      const statusClass = !pipe.enabled
        ? 'status-disabled'
        : (pipe.outgoing_connected ? 'status-connected' : 'status-disconnected');
      const statusTitle = pipe.outgoing_status
        ? ` title="${escapeHtml((pipe.input_status || '') + ' | ' + pipe.outgoing_status)}"`
        : '';
      const modeLabel = inputModeLabel(pipe.input_mode || 'tcp');

      card.innerHTML = `
        <h3>${escapeHtml(pipe.name)}</h3>
        <span class="status-badge ${statusClass}"${statusTitle}>${statusLabel}</span>
        <span class="pipe-records" title="${escapeHtml(modeLabel)}">${counterGrid(pipe.parsed_count || pipe.record_count || '0', pipe.parse_error_count || '0', pipe.sent_count || '0')}</span>
      `;

      const actions = document.createElement('div');
      actions.className = 'pipe-actions';

      const editButton = document.createElement('button');
      editButton.type = 'button';
      editButton.textContent = 'Edit';
      editButton.addEventListener('click', () => setFormPipe(pipe));
      actions.appendChild(editButton);

      const toggleButton = document.createElement('button');
      toggleButton.type = 'button';
      toggleButton.textContent = pipe.enabled ? 'Disable' : 'Enable';
      toggleButton.addEventListener('click', async () => {
        clearNotice();

        try {
          await api('/api/pipes', {
            method: 'POST',
            body: JSON.stringify({
              original_name: pipe.name,
              pipe: Object.assign({}, pipe, {
                enabled: !pipe.enabled,
                outgoing_connected: undefined,
                outgoing_status: undefined,
                input_status: undefined,
              })
            })
          });

          await loadPipes();
          showNotice(`Pipe '${pipe.name}' has been ${pipe.enabled ? 'disabled' : 'enabled'}.`, 'ok');
        } catch (err) {
          showNotice(err.message || String(err), 'error');
        }
      });
      actions.appendChild(toggleButton);

      const zeroButton = document.createElement('button');
      zeroButton.type = 'button';
      zeroButton.textContent = 'Zero';
      zeroButton.addEventListener('click', async () => {
        clearNotice();

        try {
          await api('/api/pipes/' + encodeURIComponent(pipe.name) + '/zero', { method: 'POST' });
          await loadPipes();
          showNotice(`Pipe '${pipe.name}' counters have been zeroed.`, 'ok');
        } catch (err) {
          showNotice(err.message || String(err), 'error');
        }
      });
      actions.appendChild(zeroButton);

      const deleteButton = document.createElement('button');
      deleteButton.type = 'button';
      deleteButton.textContent = 'Delete';
      deleteButton.className = 'danger';
      deleteButton.addEventListener('click', async () => {
        if (!confirm(`Delete pipe '${pipe.name}'?`)) return;

        clearNotice();

        try {
          await api('/api/pipes/' + encodeURIComponent(pipe.name), { method: 'DELETE' });
          resetForm();
          await loadPipes();
          showNotice(`Pipe '${pipe.name}' has been deleted.`, 'ok');
        } catch (err) {
          showNotice(err.message || String(err), 'error');
        }
      });
      actions.appendChild(deleteButton);

      card.appendChild(actions);

      if ((pipe.input_mode || 'tcp') === 'http_pull') {
        const pullStatuses = document.createElement('div');
        pullStatuses.className = 'pull-statuses';

        const statusByUrl = {};
        (pipe.http_pull_statuses || []).forEach(status => {
          statusByUrl[status.url] = status;
        });

        const urls = pipe.http_urls && pipe.http_urls.length
          ? pipe.http_urls
          : (pipe.http_url ? [pipe.http_url] : []);

        urls.forEach(url => {
          const status = statusByUrl[url] || {
            url: url,
            effective_url: url,
            state: 'pending',
            message: pipe.enabled ? 'not pulled yet' : 'disabled or not running',
            records: '0',
            parsed_count: '0',
            parse_error_count: '0',
            sent_count: '0',
            last_pull_at: ''
          };
          const state = status.state || 'pending';
          const stateClass = ['ok', 'error', 'pulling', 'pending'].includes(state)
            ? state
            : 'pending';
          const row = document.createElement('div');
          row.className = 'pull-status-row';
          row.title = `${status.effective_url || status.url || url} - ${status.message || ''}`;
          row.innerHTML = `
            <span class="pull-state pull-state-${stateClass}">${escapeHtml(state)}</span>
            <span class="pull-url">${escapeHtml(status.effective_url || status.url || url)}</span>
            ${counterGrid(status.parsed_count || status.records || '0', status.parse_error_count || '0', status.sent_count || '0')}
            <span class="pull-message">${escapeHtml(status.message || '')}</span>
            <span class="pull-time">${escapeHtml(formatPullTime(status.last_pull_at))}</span>
          `;
          pullStatuses.appendChild(row);
        });

        card.appendChild(pullStatuses);
      }

      container.appendChild(card);
    });
  }

  async function savePipe(event) {
    if (event) event.preventDefault();

    clearNotice();

    const form = $('pipe-form');
    if (!form.reportValidity()) {
      showNotice('Please complete the required fields before saving.', 'error');
      return;
    }

    const originalName = $('original_name').value;
    const pipe = getFormPipe();
    const wasEdit = Boolean(originalName);

    $('save-button').disabled = true;
    showNotice('Saving pipe...', 'ok');

    try {
      const result = await api('/api/pipes', {
        method: 'POST',
        body: JSON.stringify({ original_name: originalName, pipe: pipe })
      });

      resetForm();
      await loadPipes();

      showNotice(
        wasEdit
          ? `Pipe '${result.pipe.name}' has been updated.`
          : `Pipe '${result.pipe.name}' has been created.`,
        'ok'
      );
    } catch (err) {
      showNotice(err.message || String(err), 'error');
    } finally {
      $('save-button').disabled = false;
    }
  }

  $('pipe-form').addEventListener('submit', savePipe);
  $('toggle-form-button').addEventListener('click', function () {
    setFormExpanded($('pipe-form-panel').classList.contains('hidden'));
  });
  $('input_mode').addEventListener('change', updateInputModeFields);
  $('save-button').addEventListener('click', savePipe);
  $('clear-button').addEventListener('click', resetForm);
  $('add-field-button').addEventListener('click', function () {
    addFieldRow('', 'text');
  });
  $('add-tag-button').addEventListener('click', function () {
    addTagRow();
  });

  resetForm();

  loadPipes().catch(err => showNotice(err.message || String(err), 'error'));

  setInterval(() => {
    loadPipes().catch(err => showNotice(err.message || String(err), 'error'));
  }, 2000);
})();
</script>
</body>
</html>
"""


async def index(_: web.Request) -> web.Response:
    return web.Response(text=HTML_PAGE, content_type="text/html")


async def list_pipes(_: web.Request) -> web.Response:
    pipes = []
    for pipe in store.pipes.values():
        item = pipe.to_public_dict()
        item.update(manager.get_status(pipe.name))
        pipes.append(item)
    return web.json_response({"pipes": pipes})


async def save_pipe(request: web.Request) -> web.Response:
    try:
        data = await request.json()

        if "pipe" in data:
            original_name = data.get("original_name", "")
            pipe_data = dict(data["pipe"])
        else:
            original_name = data.get("name", "")
            pipe_data = dict(data)

        # Status fields are read-only.
        pipe_data.pop("outgoing_connected", None)
        pipe_data.pop("outgoing_status", None)
        pipe_data.pop("input_status", None)
        pipe_data.pop("http_pull_statuses", None)
        pipe_data.pop("record_count", None)
        pipe_data.pop("parsed_count", None)
        pipe_data.pop("parse_error_count", None)
        pipe_data.pop("sent_count", None)

        # JS may send undefined fields omitted, but remove nulls defensively.
        pipe_data = {key: value for key, value in pipe_data.items() if value is not None}

        delimiter_mode = str(pipe_data.get("delimiter_mode", "literal") or "literal")
        if delimiter_mode == "regex":
            if "delimiters" in pipe_data:
                pipe_data["delimiters"] = [
                    str(item).strip()
                    for item in pipe_data["delimiters"]
                    if str(item).strip()
                ]
            elif "delimiter" in pipe_data:
                pipe_data["delimiters"] = [str(pipe_data["delimiter"])]
        elif "delimiters" in pipe_data:
            pipe_data["delimiters"] = parse_escaped_lines(pipe_data["delimiters"])
        elif "delimiter" in pipe_data:
            pipe_data["delimiters"] = parse_escaped_lines([pipe_data["delimiter"]])
        else:
            pipe_data["delimiters"] = [","]

        if "strip_chars" in pipe_data:
            pipe_data["strip_chars"] = parse_escaped_lines(
                pipe_data["strip_chars"],
                allow_space_word=True,
            )

        if "incoming_record_delimiter" in pipe_data:
            pipe_data["incoming_record_delimiter"] = decode_escape_text(
                pipe_data["incoming_record_delimiter"] or "\\n"
            )

        if "outgoing_record_delimiter" in pipe_data:
            pipe_data["outgoing_record_delimiter"] = decode_escape_text(
                pipe_data["outgoing_record_delimiter"] or "\\n"
            )

        pipe = PipeConfig(**pipe_data)
        manager.validate_pipe_assignment(pipe, original_name=original_name)
        store.rename_or_upsert(original_name, pipe)
        await manager.sync()

        return web.json_response({"ok": True, "pipe": pipe.to_public_dict()})

    except Exception as exc:
        return web.Response(status=400, text=str(exc))


async def delete_pipe(request: web.Request) -> web.Response:
    name = request.match_info["name"]
    store.delete(name)
    await manager.sync()
    return web.json_response({"ok": True})


async def zero_pipe_counter(request: web.Request) -> web.Response:
    name = request.match_info["name"]
    try:
        manager.reset_record_count(name)
    except KeyError:
        return web.Response(status=404, text=f"Pipe '{name}' was not found.")

    return web.json_response({
        "ok": True,
        "record_count": "0",
        "parsed_count": "0",
        "parse_error_count": "0",
        "sent_count": "0",
    })


async def make_app() -> web.Application:
    store.load()
    await manager.sync()

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/api/pipes", list_pipes)
    app.router.add_post("/api/pipes", save_pipe)
    app.router.add_post("/api/pipes/{name}/zero", zero_pipe_counter)
    app.router.add_delete("/api/pipes/{name}", delete_pipe)
    return app


async def main() -> None:
    app = await make_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()

    print(f"Web UI listening on http://{WEB_HOST}:{WEB_PORT}")
    print(f"Local access: http://127.0.0.1:{WEB_PORT}")
    print(f"Remote access: http://<this-computer-ip-address>:{WEB_PORT}")

    stop_event = asyncio.Event()

    def request_shutdown() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except NotImplementedError:
            pass

    await stop_event.wait()
    print("Shutting down...")
    await manager.stop_all()
    await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
