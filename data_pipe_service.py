#!/usr/bin/env python3
"""
Data Pipe Service

Features:
- Multiple configurable TCP input pipes.
- Each pipe listens on its own TCP port.
- Incoming record delimiter is configurable per pipe; default is \n.
- Outgoing record delimiter is configurable per pipe; default is \n.
- Each pipe may define one or more field delimiters.
- Delimiters, record delimiters, and strip characters support escape notation:
  \n, \r, \t, \x1e, etc.
- Each JSON field has a configured name and type: text or numeric.
- Configured strip characters are stripped from the beginning and end of each
  input field before JSON output or numeric conversion.
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
import codecs
import json
import signal
import socket
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from aiohttp import web


CONFIG_FILE = Path("pipes_config.json")
WEB_HOST = "0.0.0.0"
WEB_PORT = 8085

CONNECT_TIMEOUT_SECONDS = 3
RECONNECT_DELAY_SECONDS = 2
CLOSE_TIMEOUT_SECONDS = 1

ALLOWED_FIELD_TYPES = {"text", "numeric"}


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


@dataclass
class PipeConfig:
    name: str
    listen_host: str = "0.0.0.0"
    listen_port: int = 9000
    outgoing_host: str = "127.0.0.1"
    outgoing_port: int = 9100

    # Backward-compatible single field delimiter.
    delimiter: str = ","

    # New multi-delimiter and record-delimiter settings.
    delimiters: List[str] = field(default_factory=list)
    incoming_record_delimiter: str = "\n"
    outgoing_record_delimiter: str = "\n"
    strip_chars: List[str] = field(default_factory=list)

    field_names: List[str] = field(default_factory=list)
    field_types: List[str] = field(default_factory=list)

    enabled: bool = True

    def __post_init__(self) -> None:
        if not self.delimiters:
            self.delimiters = [self.delimiter or ","]

        # Values may already be decoded from config, or escaped from API.
        self.delimiters = [decode_escape_text(d) for d in self.delimiters]
        self.incoming_record_delimiter = decode_escape_text(self.incoming_record_delimiter or "\\n")
        self.outgoing_record_delimiter = decode_escape_text(self.outgoing_record_delimiter or "\\n")
        self.strip_chars = [decode_escape_text(c) for c in self.strip_chars]

        if not self.field_types:
            self.field_types = ["text"] * len(self.field_names)

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("Pipe name is required.")

        if not self.delimiters:
            raise ValueError("At least one field delimiter is required.")

        for delimiter in self.delimiters:
            if delimiter == "":
                raise ValueError("Field delimiters cannot be empty.")

        if self.incoming_record_delimiter == "":
            raise ValueError("Incoming record delimiter cannot be empty.")

        if self.outgoing_record_delimiter == "":
            raise ValueError("Outgoing record delimiter cannot be empty.")

        self.listen_port = int(self.listen_port)
        self.outgoing_port = int(self.outgoing_port)

        if not (1 <= self.listen_port <= 65535):
            raise ValueError("Listen port must be between 1 and 65535.")

        if not (1 <= self.outgoing_port <= 65535):
            raise ValueError("Outgoing port must be between 1 and 65535.")

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

    def to_public_dict(self) -> dict:
        item = asdict(self)
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
    def __init__(self, config: PipeConfig):
        self.config = config
        self.server: Optional[asyncio.AbstractServer] = None

        self.outgoing_reader: Optional[asyncio.StreamReader] = None
        self.outgoing_writer: Optional[asyncio.StreamWriter] = None
        self.outgoing_connected = False
        self.outgoing_status = "not started"

        self.reconnect_task: Optional[asyncio.Task] = None
        self.outgoing_monitor_task: Optional[asyncio.Task] = None

        self.send_lock = asyncio.Lock()
        self.stopping = False

    async def start(self) -> None:
        if self.server is not None:
            return

        self.stopping = False
        self.server = await asyncio.start_server(
            self.handle_client,
            self.config.listen_host,
            self.config.listen_port,
        )
        self.reconnect_task = asyncio.create_task(self.reconnect_loop())

        sockets = ", ".join(str(sock.getsockname()) for sock in self.server.sockets or [])
        print(f"Pipe '{self.config.name}' listening on {sockets}")

    async def stop(self) -> None:
        self.stopping = True

        if self.reconnect_task is not None:
            self.reconnect_task.cancel()
            try:
                await self.reconnect_task
            except asyncio.CancelledError:
                pass
            self.reconnect_task = None

        await self.close_outgoing()

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        self.outgoing_status = "stopped"
        print(f"Pipe '{self.config.name}' stopped")

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        print(f"Pipe '{self.config.name}' accepted connection from {peer}")

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
                    if not record:
                        continue

                    try:
                        json_record = self.record_to_json(record)
                        await self.send_outgoing(json_record)
                    except Exception as exc:
                        print(f"Pipe '{self.config.name}' failed record {record!r}: {exc}")
        finally:
            writer.close()
            await writer.wait_closed()
            print(f"Pipe '{self.config.name}' closed connection from {peer}")

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
        }

    def record_to_json(self, record: str) -> str:
        fields = self.split_record(record)
        strip_set = "".join(self.config.strip_chars)

        obj = {}
        for index, value in enumerate(fields):
            if strip_set:
                value = value.strip(strip_set)

            if index < len(self.config.field_names):
                key = self.config.field_names[index]
                field_type = self.config.field_types[index]
            else:
                key = f"field_{index + 1}"
                field_type = "text"

            if field_type == "numeric":
                obj[key] = self.parse_numeric(value, key)
            else:
                obj[key] = value

        return json.dumps(obj, separators=(",", ":")) + self.config.outgoing_record_delimiter

    def split_record(self, record: str) -> List[str]:
        """
        Split one record using one or more delimiters.

        Double-quoted text is supported. Delimiters inside quoted text are
        preserved. Doubled quotes inside quoted text become one quote.
        """
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


class PipeManager:
    def __init__(self, store: ConfigStore):
        self.store = store
        self.runtimes: Dict[str, PipeRuntime] = {}

    @staticmethod
    def is_port_available(host: str, port: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
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

            if int(existing_pipe.listen_port) == int(pipe.listen_port):
                raise ValueError(
                    f"Incoming port {pipe.listen_port} is already assigned "
                    f"to pipe '{existing_name}'."
                )

        active_same_pipe = self.runtimes.get(original_name or pipe.name)
        same_active_port = (
            active_same_pipe is not None
            and int(active_same_pipe.config.listen_port) == int(pipe.listen_port)
            and active_same_pipe.config.listen_host == pipe.listen_host
        )

        if pipe.enabled and not same_active_port:
            if not self.is_port_available(pipe.listen_host, pipe.listen_port):
                raise ValueError(
                    f"Incoming port {pipe.listen_port} on {pipe.listen_host} "
                    f"is already in use."
                )

    async def sync(self) -> None:
        desired_names = set(self.store.pipes.keys())
        active_names = set(self.runtimes.keys())

        for name in active_names - desired_names:
            await self.runtimes[name].stop()
            del self.runtimes[name]

        for name, config in self.store.pipes.items():
            existing = self.runtimes.get(name)

            if existing is not None and asdict(existing.config) != asdict(config):
                await existing.stop()
                del self.runtimes[name]
                existing = None

            if config.enabled and existing is None:
                runtime = PipeRuntime(config)
                await runtime.start()
                self.runtimes[name] = runtime

            if not config.enabled and existing is not None:
                await existing.stop()
                del self.runtimes[name]

    def get_status(self, name: str) -> dict:
        runtime = self.runtimes.get(name)
        if runtime is None:
            return {
                "outgoing_connected": False,
                "outgoing_status": "disabled or not running",
            }
        return runtime.status()

    async def stop_all(self) -> None:
        for runtime in list(self.runtimes.values()):
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
    label { display: block; margin-top: .45rem; font-weight: 600; }
    input, select, textarea { width: 100%; box-sizing: border-box; padding: .35rem .45rem; margin-top: .15rem; }
    textarea { min-height: 3rem; font-family: monospace; }
    button { margin-top: .45rem; margin-right: .35rem; padding: .4rem .7rem; border: 0; border-radius: 6px; cursor: pointer; }
    button:disabled { opacity: .6; cursor: not-allowed; }
    table { width: 100%; border-collapse: collapse; margin-top: .45rem; }
    th, td { border-bottom: 1px solid #ddd; padding: .3rem .4rem; text-align: left; vertical-align: top; }
    .primary { background: #1f6feb; color: white; }
    .danger { background: #c62828; color: white; }
    .muted { color: #666; }
    .notice { display: none; padding: .5rem .7rem; border-radius: 6px; margin: .6rem 0; }
    .notice.ok { display: block; background: #e8f5e9; border: 1px solid #a5d6a7; }
    .notice.error { display: block; background: #ffebee; border: 1px solid #ef9a9a; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: .65rem; }
    code { background: #eee; padding: .15rem .3rem; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Data Pipe Configuration</h1>
  <p class="muted">Records are received over TCP and transformed into delimited JSON records.</p>
  <div id="notice" class="notice"></div>

  <div class="card">
    <h2 id="form-title">Add Pipe</h2>
    <form id="pipe-form">
      <input type="hidden" id="original_name">

      <label for="name">Name</label>
      <input id="name" required placeholder="orders_pipe">

      <div class="row">
        <div>
          <label for="listen_host">Listen Host</label>
          <input id="listen_host" value="0.0.0.0">
        </div>
        <div>
          <label for="listen_port">Listen Port</label>
          <input id="listen_port" type="number" min="1" max="65535" required value="9000">
        </div>
      </div>

      <div class="row">
        <div>
          <label for="outgoing_host">Outgoing Host</label>
          <input id="outgoing_host" required value="127.0.0.1">
        </div>
        <div>
          <label for="outgoing_port">Outgoing Port</label>
          <input id="outgoing_port" type="number" min="1" max="65535" required value="9100">
        </div>
      </div>

      <div class="row">
        <div>
          <label for="incoming_record_delimiter">Incoming Record Delimiter</label>
          <input id="incoming_record_delimiter" value="\n">
        </div>
        <div>
          <label for="outgoing_record_delimiter">Outgoing Record Delimiter</label>
          <input id="outgoing_record_delimiter" value="\n">
        </div>
      </div>
      <p class="muted">Use escape notation such as <code>\n</code>, <code>\r</code>, <code>\t</code>, or <code>\x1e</code>.</p>

      <label for="delimiters">Field Delimiters</label>
      <textarea id="delimiters" placeholder="One delimiter per line. Examples:
,
|
\t
\x1e">,</textarea>
      <p class="muted">One delimiter per line. Delimiters inside double quotes are ignored.</p>

      <label for="strip_chars">Characters to Strip From JSON Values</label>
      <textarea id="strip_chars" placeholder="One per line. Examples:
space
\t
\r
\n">space</textarea>
      <p class="muted">Use <code>space</code> for a space character. These characters are stripped from the beginning and end of each field.</p>

      <h3>JSON Fields</h3>
      <p class="muted">Add one JSON field for each input column. Choose whether each output value is text or numeric.</p>
      <table>
        <thead><tr><th>JSON field name</th><th>Type</th><th></th></tr></thead>
        <tbody id="fields-body"></tbody>
      </table>
      <button id="add-field-button" type="button">Add JSON Field</button>

      <label>
        <input id="enabled" type="checkbox" checked style="width:auto;">
        Enabled
      </label>

      <button id="save-button" class="primary" type="submit">Save Pipe</button>
      <button id="clear-button" type="button">Clear Form</button>
    </form>
  </div>

  <div class="card">
    <h2>Existing Pipes</h2>
    <p class="muted">Use this section to edit, enable, disable, or delete configured data pipes.</p>
    <div id="pipes">Loading pipes...</div>
  </div>

<script>
(function () {
  const $ = id => document.getElementById(id);

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

  function getFormPipe() {
    const fields = getFields();
    const delimiters = linesFromTextarea('delimiters');

    return {
      name: $('name').value.trim(),
      listen_host: $('listen_host').value.trim() || '0.0.0.0',
      listen_port: Number($('listen_port').value),
      outgoing_host: $('outgoing_host').value.trim(),
      outgoing_port: Number($('outgoing_port').value),
      incoming_record_delimiter: $('incoming_record_delimiter').value || '\\n',
      outgoing_record_delimiter: $('outgoing_record_delimiter').value || '\\n',
      delimiter: delimiters[0] || ',',
      delimiters: delimiters,
      strip_chars: linesFromTextarea('strip_chars'),
      field_names: fields.names,
      field_types: fields.types,
      enabled: $('enabled').checked
    };
  }

  function setFormPipe(pipe) {
    $('form-title').textContent = 'Edit Pipe';
    $('original_name').value = pipe.name;
    $('name').value = pipe.name;
    $('listen_host').value = pipe.listen_host;
    $('listen_port').value = pipe.listen_port;
    $('outgoing_host').value = pipe.outgoing_host;
    $('outgoing_port').value = pipe.outgoing_port;
    $('incoming_record_delimiter').value = pipe.incoming_record_delimiter || '\\n';
    $('outgoing_record_delimiter').value = pipe.outgoing_record_delimiter || '\\n';
    $('delimiters').value = (pipe.delimiters && pipe.delimiters.length ? pipe.delimiters : [pipe.delimiter || ',']).join('\n');
    $('strip_chars').value = (pipe.strip_chars && pipe.strip_chars.length ? pipe.strip_chars : []).join('\n');
    $('enabled').checked = Boolean(pipe.enabled);

    $('fields-body').innerHTML = '';
    (pipe.field_names || []).forEach((fieldName, index) => {
      const fieldType = (pipe.field_types || [])[index] || 'text';
      addFieldRow(fieldName, fieldType);
    });

    if ((pipe.field_names || []).length === 0) {
      addFieldRow('', 'text');
    }

    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function resetForm() {
    $('form-title').textContent = 'Add Pipe';
    $('original_name').value = '';
    $('pipe-form').reset();
    $('listen_host').value = '0.0.0.0';
    $('listen_port').value = '9000';
    $('outgoing_host').value = '127.0.0.1';
    $('outgoing_port').value = '9100';
    $('incoming_record_delimiter').value = '\\n';
    $('outgoing_record_delimiter').value = '\\n';
    $('delimiters').value = ',';
    $('strip_chars').value = 'space';
    $('enabled').checked = true;
    $('fields-body').innerHTML = '';
    addFieldRow('', 'text');
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
      card.className = 'card';

      const fieldsText = (pipe.field_names || []).map((name, index) => {
        const type = (pipe.field_types || [])[index] || 'text';
        return `${name}:${type}`;
      }).join(', ') || '(auto field_1:text, field_2:text, ...)';

      const delimitersText = (
        pipe.delimiters && pipe.delimiters.length ? pipe.delimiters : [pipe.delimiter || ',']
      ).join(' | ');

      const stripText = (pipe.strip_chars || []).join(' | ') || '(none)';

      card.innerHTML = `
        <h3>${escapeHtml(pipe.name)}</h3>
        <p><strong>Status:</strong> ${pipe.enabled ? 'Enabled and listening' : 'Disabled'}</p>
        <p><strong>Endpoint:</strong> ${pipe.outgoing_connected ? 'Connected' : 'Disconnected'} — <code>${escapeHtml(pipe.outgoing_status || '')}</code></p>
        <p><strong>Input:</strong> <code>${escapeHtml(pipe.listen_host)}:${escapeHtml(pipe.listen_port)}</code></p>
        <p><strong>Output:</strong> <code>${escapeHtml(pipe.outgoing_host)}:${escapeHtml(pipe.outgoing_port)}</code></p>
        <p><strong>Incoming record delimiter:</strong> <code>${escapeHtml(pipe.incoming_record_delimiter || '\\n')}</code></p>
        <p><strong>Outgoing record delimiter:</strong> <code>${escapeHtml(pipe.outgoing_record_delimiter || '\\n')}</code></p>
        <p><strong>Field delimiters:</strong> <code>${escapeHtml(delimitersText)}</code></p>
        <p><strong>Strip chars:</strong> <code>${escapeHtml(stripText)}</code></p>
        <p><strong>JSON fields:</strong> <code>${escapeHtml(fieldsText)}</code></p>
      `;

      const editButton = document.createElement('button');
      editButton.type = 'button';
      editButton.textContent = 'Edit';
      editButton.addEventListener('click', () => setFormPipe(pipe));
      card.appendChild(editButton);

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
                outgoing_status: undefined
              })
            })
          });

          await loadPipes();
          showNotice(`Pipe '${pipe.name}' has been ${pipe.enabled ? 'disabled' : 'enabled'}.`, 'ok');
        } catch (err) {
          showNotice(err.message || String(err), 'error');
        }
      });
      card.appendChild(toggleButton);

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
      card.appendChild(deleteButton);

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
  $('save-button').addEventListener('click', savePipe);
  $('clear-button').addEventListener('click', resetForm);
  $('add-field-button').addEventListener('click', function () {
    addFieldRow('', 'text');
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

        # JS may send undefined fields omitted, but remove nulls defensively.
        pipe_data = {key: value for key, value in pipe_data.items() if value is not None}

        if "delimiters" in pipe_data:
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


async def make_app() -> web.Application:
    store.load()
    await manager.sync()

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/api/pipes", list_pipes)
    app.router.add_post("/api/pipes", save_pipe)
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
