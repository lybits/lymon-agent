# S7 connector plugin — design spec

**Date:** 2026-06-18
**Status:** Approved design, pending spec review
**Component:** `lymon-agent/crates/plugin-s7`
**Author:** Lymon team (brainstormed with Claude)

## 1. Goal & scope

Graduate `plugin-s7` from a skeleton (returns deterministic fake values) into a
real connector that reads live values from Siemens S7-300/400/1200/1500 PLCs
over ISO-on-TCP (S7comm, TCP/102), with:

- **Full addressing** — memory areas DB / M (Merker) / I (inputs) / Q (outputs),
  all common numeric types, and bit access.
- **Discover** — enumerate the CPU's data blocks (numbers + sizes) to power the
  portal source explorer.

Non-goals: write/actuation, `subscribe`/push (S7comm is poll-only — the agent
polls via `read`), reading string/char types, and reading *optimized* DBs
(unreachable by absolute addressing — see §5).

## 2. Current state

`plugin-s7` is a workspace member built on `lymon-collector-sdk`. Its
`Collector::read` already parses the right contract (`config {host,rack,slot}`,
`selection {db,byte,type}`) but the wire read is a placeholder. The sibling
`plugin-opcua` is the fully-implemented reference (cached session, reconnect on
error, discover) and establishes the patterns this plugin mirrors. Unlike
`plugin-opcua` (excluded from the workspace for its heavy async-opcua + crypto
tree), `plugin-s7` stays **in the workspace** — see §3/§4.

## 3. Decision: vendor `rust7`, extend it

The S7 client is **[rust7](https://github.com/davenardella/rust7)** by Davide
Nardella (the author of snap7), vendored into the plugin and extended.

- **Why rust7:** pure-Rust, **zero dependencies** (std-only), **blocking** I/O —
  a perfect fit for the synchronous `Collector::read` running in the plugin's own
  process (no tokio, unlike OPC-UA). MIT-licensed (vendor/extend freely). It is a
  faithful port of snap7's core by snap7's own author: correct ISO-on-TCP/COTP/S7
  framing, automatic PDU chunking, explicit `S7Error`. It already covers the
  entire **read** scope (areas DB/M/I/Q via `read_area`, bit access via
  `read_bit`).
- **Why vendor (copy in) rather than depend:** rust7 has no block-listing and its
  `TcpStream` is a private field with no public "exchange" method, so **discover
  cannot be added from outside the crate**. It is a single ~37 KB MIT file; copying
  it in lets us add the block-list telegram, keeps the plugin pure-Rust and *in
  the workspace* with no C toolchain, and we can offer the additions upstream.
- **Alternatives rejected:** `snap7-rs` (C++ bindings — discover out of the box,
  but drags in a C++ build toolchain and the exclude-from-workspace treatment);
  depending on rust7 unmodified (lightest, but discover is impossible without
  forking); hand-rolling S7comm (most work, no benefit over porting snap7's
  proven telegrams).

## 4. Architecture & code layout

```
crates/plugin-s7/
  Cargo.toml          # deps UNCHANGED: lymon-collector-sdk + serde_json
                      # (rust7 is std-only → no new external deps)
  plugin.json         # unchanged (name lymon-plugin-s7, types:["s7"], protocol 1); bump version
  src/
    main.rs           # Collector impl: connection cache, read, discover, error mapping
    s7/
      mod.rs          # re-exports
      client.rs       # VENDORED rust7 client.rs (MIT header + attribution preserved)
                      #   carries two clearly-marked `// LYMON DELTA:` fixes (see §8)
      blocks.rs       # OUR extension: list_blocks_of_type() + get_block_info() for discover
      decode.rs       # type decoders (big-endian bytes → f64), pure + unit-tested
```

`plugin-s7` remains a workspace member; CI already builds and packages it
(commits `dca5735`, `6b0ad56`). No tokio, no C toolchain, no new crates.

## 5. Connector contract

**config** (per connector):
```json
{ "host": "192.168.2.234", "rack": 0, "slot": 1 }
```
`rack`/`slot` default `0`/`1`; passed to `connect_rack_slot(host, rack, slot)`.
(For S7-1500 the working slot is 0 or 1 depending on firmware — the classic snap7
ambiguity; defaulting to 1, overridable, confirmed empirically against the sim and
documented in the runbook.)

**selection** (per ingest):
```json
{ "area": "db", "db": 1, "byte": 0, "bit": 0, "type": "real" }
```
- `area`: `db` (default) | `m`/`merker` | `i`/`input` | `q`/`output`. `db` is
  required only when `area == db`.
- `byte`: byte offset within the area. `bit`: 0–7, used only for `bool`.
- `type` (decode; **S7 is big-endian**), aliases keep today's names working:

| `type` | size | decode |
|---|---|---|
| `bool` | 1 bit | `read_bit` → 0.0 / 1.0 |
| `sint` | 1 | `i8` |
| `byte` / `usint` | 1 | `u8` |
| `int` | 2 | `i16` BE |
| `word` / `uint` | 2 | `u16` BE |
| `dint` | 4 | `i32` BE |
| `dword` / `udint` | 4 | `u32` BE |
| `real` | 4 | `f32` BE |
| `lreal` | 8 | `f64` BE |

Every value is coerced to the `Sample`'s `f64` (the warehouse stores numeric
samples), mirroring `plugin-opcua`'s `variant_to_f64`. An unknown `type` or an
out-of-range `bit` is a clean error, never a panic.

**Optimized-DB constraint:** absolute addressing reaches a DB only when the DB has
**"Optimized block access" OFF** (standard access) **and** the CPU permits
**PUT/GET** from a remote partner. Optimized DBs surface as `S7InvalidAddress`,
mapped to an actionable message (§9).

## 6. `read` flow & connection lifecycle

```rust
use crate::s7::client::S7Client;     // vendored, not an external crate dependency

struct S7Connector {
    conn: Option<S7Client>,           // one cached, live session
    key:  Option<(String, u16, u16)>, // (host, rack, slot) the session is for
}
```

Per `read`: ensure a session (connect if `conn` is `None` or `key` changed),
compute the byte length from `type`, call `read_area`/`read_bit`, decode → f64 →
`Sample::new(variable_id, value)`. Connection timeouts are set explicitly
(rust7 defaults 3000/1000/500 ms) so a poll never hangs silently — the lesson
banked from the OPC-UA connect-timeout fix (#11). On **any** connect/read error
the cached session is dropped so the next poll reconnects cleanly — the
`plugin-opcua` pattern.

## 7. `discover` design

`blocks.rs` ports snap7's block-listing into the vendored client (the client's
private socket is accessible from within the same module):

- **List blocks of type** — an S7 *userdata* PDU, function group "block functions",
  subfunction "list blocks of type" with block type `DB`. Returns the list of DB
  numbers present in the CPU. Byte layout ported from snap7's `ListBlocksOfType`
  and validated against a Wireshark S7comm trace.
- **Get block info** (optional, per DB) — subfunction "get block info" for each DB
  to report its size.

`discover` returns:
```
schema_kind: "s7_blocks"
nodes: [ leaf(id="DB1", label="DB1 (512 bytes)", node_type="db", meta:{db:1,size_bytes:512}), … ]
```

**Honest limitation:** S7 discover is *coarse* — it lists DB numbers/sizes, not
field layouts (symbolic structure lives in the offline TIA project, not on the
wire). The portal user still enters `byte` + `type`. Scope for this iteration is
**DB enumeration only**; surfacing static M/I/Q area nodes as addressing hints is a
cheap later nicety. `query`/`test` ("Test selection") come free from the SDK
default (one `read` shaped as a scalar).

## 8. Vendored-code deltas (incorporated fixes)

Two surgical, `// LYMON DELTA:`-marked edits over verbatim rust7 `client.rs`, kept
minimal so re-syncing upstream stays a clean diff. Both are candidates to offer
back to Davide as PRs.

1. **`impl std::error::Error for S7Error`** (rust7 issue #5 / PR #6, unmerged
   upstream). The `Display` impl exists; add the marker impl plus `source()`
   returning the inner `io::Error` for the `Io` variant. Lets the error compose
   with `Box<dyn Error>` / `anyhow`. (Our plugin maps to `String` for the SDK, so
   this is hygiene, not strictly required — but cheap and correct.)
2. **Harden the S7 response read.** `read_area` reads the S7 payload with a single
   `stream.read()` and errors on a short read; TCP may return a partial segment,
   so a large read near the negotiated PDU size can spuriously fail. Replace with
   `read_exact` of the expected `s7_comm_size`. Apply the same hardening to the
   write-response and PDU-negotiation reads. Negligible for small scalars; matters
   for reliability under 24/7 polling and for multi-byte discover responses.

rust7's `write_area`/`write_db`/`write_bit` are kept verbatim (unused — write is a
non-goal) so the vendored file stays a clean diff against upstream; the §8.2
hardening still applies to their response reads for consistency and upstreaming.

No *closed* rust7 issue touches `client.rs`, so nothing fixed-upstream is being
missed by vendoring the current file.

## 9. Error handling

rust7's `S7Error` variants map to clear, actionable strings; two get special care:

- `S7InvalidAddress` → *"address out of range, or the DB is optimized — switch the
  DB to standard (non-optimized) access in TIA Portal."*
- `S7NotFound` → *"DB N does not exist in the CPU."*

Connection/IO errors drop the cached session (reconnect next poll). Malformed
selections (wrong type, bit > 7, missing `db` for a DB read) error before any
wire I/O.

## 10. Testing & sim runbook

**Unit (CI, no PLC):**
- `decode.rs` — table-driven tests: every type, big-endian byte order, negatives,
  `real`/`lreal` round-trips, NaN handling.
- selection/area/type parsing and error mapping; malformed-input pass (non-numeric,
  oversized, missing fields) asserting clean errors rather than panics — banking the
  "seam input-validation" lesson.

**Live (manual, against the `Grabo` S7-1500 sim @ 192.168.2.234):** a `RUNBOOK.md`
covering:
- **TIA setup:** one **standard-access** test DB; CPU Protection → **Full access**
  + **Permit access with PUT/GET communication**. (Verify/flip on the existing
  program.)
- **Known test layout**, e.g. `DB1`: `REAL`@0 = 3.14, `INT`@4 = 42, `BOOL`@6.0 =
  true.
- **Drive the plugin directly** by piping JSON-lines, bypassing the agent for fast
  iteration:
  `echo '{"v":1,"op":"read","type":"s7","config":{"host":"192.168.2.234","rack":0,"slot":1},"selection":{"area":"db","db":1,"byte":0,"type":"real"},"naming":{"variable_id":"t.real"}}' | ./lymon-plugin-s7`
- **Discover check:** `op:"discover"` lists the DBs; cross-check the block-list
  telegram against a Wireshark S7comm capture.
- **Slot check:** confirm whether slot 0 or 1 connects to the S7-1500; record it.

## 11. Phasing

Both phases land this iteration but are independently testable:

- **Phase 1 — Read.** Vendor `client.rs` (+ the two §8 deltas), `decode.rs`,
  connection cache, `read` for all areas/types/bits. Verifiable against the sim
  immediately.
- **Phase 2 — Discover.** `blocks.rs` list-blocks/get-block-info telegrams +
  `discover`. The one novel protocol piece, isolated and Wireshark-validated.

## 12. Open items / risks

- **Block-list telegram** is the only new protocol surface; de-risked by porting
  from snap7's reference and validating against a live trace.
- **S7-1500 slot** (0 vs 1) confirmed empirically against the sim.
- **Optimized DBs** are out of reach by design; the runbook makes the TIA settings
  explicit so testing isn't blocked.

## 13. References & attribution

- rust7 — https://github.com/davenardella/rust7 (MIT © 2025 Davide Nardella).
  Vendored `client.rs` retains its copyright/license header.
- snap7 — https://snap7.sourceforge.net (block-listing telegram reference).
- snap7 PUT/GET + non-optimized-DB requirement —
  https://www.solisplc.com/tutorials/introduction-to-snap7-integration-into-siemens-tia-portal
- Patterns mirrored from `crates/plugin-opcua` (cached session, reconnect-on-error,
  discover) and `crates/collector-sdk` (the `Collector` contract).
