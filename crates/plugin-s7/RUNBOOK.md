# lymon-plugin-s7 — RUNBOOK

How to build the Siemens S7 connector plugin and verify it against a live PLC or a
**S7-PLCSIM Advanced** instance. The plugin speaks S7comm (ISO-on-TCP, TCP/102) using a
vendored [`rust7`](https://github.com/davenardella/rust7) client.

> **Why this runbook exists:** CI builds and unit-tests the plugin, but the wire read and
> the discover telegram can only be confirmed against a real CPU/sim. This is that check.

---

## 1. Prerequisites

- A reachable S7-1200/1500 (or 300/400) CPU, **or** S7-PLCSIM Advanced V4.0+ running an
  instance with the **PLCSIM Virtual Eth. Adapter** (so it has a real IP, e.g. `192.168.2.234`).
- The Rust toolchain (`rustup`, stable) on the build host.

## 2. Make the CPU readable (S7-1500 / S7-1200)

Classic absolute addressing (`DB1.DBW4`-style) only works when **both** are true. Set them in
TIA Portal and re-download:

1. **Data block → Standard access.** Right-click the DB → *Properties* → *Attributes* →
   **uncheck "Optimized block access"**. (Optimized DBs are unreachable by `db+byte` and surface
   as an `invalid address` error.)
2. **CPU → allow PUT/GET.** CPU *Properties* → *Protection & Security* → *Connection mechanisms*
   → **check "Permit access with PUT/GET communication from remote partner"**; set the access
   level to **Full access** if reads still fail.

Only **global DBs** are readable this way.

## 3. A known test DB

Create (or reuse) a standard-access global DB with known values, e.g. `DB1`:

| Address    | TIA type | value  | plugin `selection`                                   |
|------------|----------|--------|------------------------------------------------------|
| `DB1.DBD0` | Real     | 3.14   | `{"area":"db","db":1,"byte":0,"type":"real"}`        |
| `DB1.DBW4` | Int      | 42     | `{"area":"db","db":1,"byte":4,"type":"int"}`         |
| `DB1.DBX6.0` | Bool   | TRUE   | `{"area":"db","db":1,"byte":6,"bit":0,"type":"bool"}`|

## 4. Build

```bash
cargo build --release -p lymon-plugin-s7
# binary: target/release/lymon-plugin-s7  (lymon-plugin-s7.exe on Windows)
```

## 5. Drive it directly (bypass the agent)

The plugin is a JSON-lines stdio program: one request line in → one response line out. Pipe a
request to it for a fast read/connectivity check (replace the host with your CPU/sim IP).

**Read REAL @ DB1.0** (bash):
```bash
echo '{"v":1,"op":"read","type":"s7","config":{"host":"192.168.2.234","family":"s7-1500"},"selection":{"area":"db","db":1,"byte":0,"type":"real"},"naming":{"variable_id":"t.real"}}' \
  | ./target/release/lymon-plugin-s7
# → {"ok":true,"samples":[{"variable_id":"t.real","value":3.14,"quality":0}]}
```

**PowerShell** (Windows):
```powershell
'{"v":1,"op":"read","type":"s7","config":{"host":"192.168.2.234","family":"s7-1500"},"selection":{"area":"db","db":1,"byte":4,"type":"int"},"naming":{"variable_id":"t.int"}}' `
  | .\target\release\lymon-plugin-s7.exe
```

**Discover (list DBs):**
```bash
echo '{"v":1,"op":"discover","type":"s7","config":{"host":"192.168.2.234","family":"s7-1500"},"args":{}}' \
  | ./target/release/lymon-plugin-s7
# → {"ok":true,"result":{"kind":"tree","schema_kind":"s7_blocks","nodes":[
#     {"id":"DB1","label":"DB1 · MotorData (512 B)","node_type":"db",
#      "meta":{"db":1,"name":"MotorData","size_bytes":512,"lang":4}}, ...]}}
# (name comes from GetAgBlockInfo's 8-char header field — often blank on optimized S7-1500 DBs)
```

**Test a selection** (`op:"test"` returns one scalar, used by the portal "Test selection"):
```bash
echo '{"v":1,"op":"test","type":"s7","config":{"host":"192.168.2.234","family":"s7-1500"},"args":{"area":"db","db":1,"byte":0,"type":"real"}}' \
  | ./target/release/lymon-plugin-s7
```

## 6. Connection config reference

`config`:
- `host` (required) — CPU/sim IP.
- `family` — `s7-1500` / `s7-1200` (rack 0, slot 1), `s7-300` (rack 0, slot 2). Resolves rack/slot.
- `rack` / `slot` — explicit override (any pair). For `s7-400` set these explicitly.

**Slot gotcha:** for S7-1200/1500 the working slot is **0 or 1** depending on firmware. `family`
defaults to slot **1**; if the connection is refused, retry with `{"rack":0,"slot":0}`. Record
the value that works for your CPU here: `__________`.

## 7. Validate the discover telegram (first bring-up only)

The "list blocks of type" telegram is ported from snap7's `opListBlocksOfType` and unit-tested
at the parser level, but the wire exchange is best confirmed once with a capture:

1. Start Wireshark on the adapter facing the CPU, filter `s7comm || cotp`.
2. Run the discover command above.
3. Confirm the response is a **Userdata → Block functions → List blocks of type** ACK whose data
   section lists your DB numbers. The decoded `nodes` should match the DBs in the project.

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `invalid address ... DB is optimized` | The DB has Optimized access on → set Standard access (§2.1). |
| `does not exist in the CPU` | Wrong DB number, or the DB isn't downloaded. |
| connect times out / refused | Wrong IP; PUT/GET not permitted (§2.2); wrong slot (§6) — try slot 0 vs 1. |
| `value` is wrong/garbage | Wrong `type` or `byte` offset; remember S7 is big-endian and `byte` is the absolute DB offset. |
| discover returns `[]` | CPU returned no DBs of that type, or a multi-slice reply (only the first slice is read today). |
