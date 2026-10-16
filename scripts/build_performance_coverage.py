"""Build the card's numeric coverage ledger from supported attachment exports.

Inputs are findings data. No PA runtime, configuration, storage or peer is opened.
Only selected numeric summaries are copied; prompts/transcripts/examples are excluded.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

CARD = "7894e44b-eef9-4da8-9f7f-7d3ae82c0b54"
BACKUP = "0331d21a-f014-44c8-9176-bc1e955a9ad2"
DATABASE = "89957d94-db93-4bf4-b534-3fbea8ba783c"
HEAD = "630890e3e7da7dd18a18bbd69b98120ba3c134fc"
MERGE = "2b601f94e1498de1299274ba95065b817a6fd133"

FAMILIES = {
    "sync": ("Confirmed shared critical-section and ancestry-query amplification in isolated Kernel fixtures; not every historical outlier attributed.", "PR484/485: suffix/index publication outside primary admission, forward ancestry query, scoped derived connections; both-parent and receipt regressions.", "Exercise remaining sync reads/object collection and every affected waiter under mixed load; inspect supported peer evidence."),
    "sqlite": ("Binding's historical lock wait dominates its own hold. Other SQLite tails require per-operation attribution; native busy vs disk wait remains unpartitioned.", "PR484/485 shorten shared mutation admission and batch transcript writes; numeric SQL/fetch/lock metrics.", "Run each uncovered SQL operation with concurrent sync/transcript/control traffic; measure native busy and physical writes without changing durability."),
    "acp": ("Transcript/wire serialization and shared contention are candidates; isolated batching/order verified, historical external provider tails not fully attributed.", "PR484 bounded transcript batches and awaited durable terminal checkpoint; fixture durability/order checks.", "Verify wire read/write, cancellation, reconnect and real-provider startup/drain separately from fixture provider."),
    "progress": ("Durable checkpoint/sequence work and repeated hydration can amplify contention; enqueue remains a source-inspected synchronous path.", "PR484 awaits terminal checkpoint; PR485 compact dispatch status and candidate scans.", "Measure normal enqueue, heartbeat, sequence and checkpoints under mixed load; preserve fences and durable acknowledgement."),
    "dispatch": ("Confirmed repeated copies of retained execution/progress evidence; not every admission/capacity tail attributed.", "PR485 compact list/status and detached candidate DTO; 50-record response 20,275,331 to123,140bytes (20requests).", "Measure normal admission/capacity expiry/cancel/follow-up/receipt paths and preserve fresh-record checks."),
    "reconciliation": ("Confirmed full-record candidate hydration; other dependency and lifecycle delays remain unverified.", "PR485 identity-only candidates, fresh reload before actions, heartbeat only after completed candidate.", "Exercise due/completion/dispatch reads with realistic retained proofs and stalled external dependencies."),
    "fleet": ("Some local status/copy amplification confirmed; peer failures and provider/probe service time not causally attributed.", "PR484 scopes cached harness catalog; PR485 bounded dispatch list. Peer failure handling remains open.", "Measure placement/provider/catalog/overview branches; separate external probes and local CPU; avoid changing admission semantics."),
    "agent": ("Shared lock and hydration candidates; fixture cold start is not real-provider/network acceptance.", "PR484/485 binding contention, compact session/sidebar and transcript fixes.", "Measure recovery/history detail paging, actual MCP/provider/workspace phases and normal prompt admission."),
    "pr_supervisor": ("External GitHub/peer time and local ledger cost must be separated; historical maxima alone do not prove cause.", "Existing durable PR supervisor retained; no blanket latency resolution claimed.", "Measure external snapshot vs local lease/read/write/effects; verify bounded repeated-failure/backoff behavior."),
    "workspace": ("Git/filesystem/maintenance costs vs lease-store contention unresolved.", "No new workspace repair claimed; fencing and durability unchanged.", "Profile isolated representative provision/renew/GC/maintenance with live-worktree fencing assertions."),
}

CONCERNS = [
    ("global-lock", "Sync/global mutation lock", "sync", "Historical push219.9s, binding wait197.9s vs own hold<=21.3ms; sync lock-hold p99~73.95s. Exact merged-tree 100push/100reconcile mixed fixture passes zero slow calls. Remaining lock waiters stay open."),
    ("payloads", "Payload and copy amplification", "agent", "Session~1.19MBp95; dispatch22.4MB; history~860KBp95; recovery2.83MB. Dispatch list reduction measured; history/recovery detail contracts still need bounded paging/measurement."),
    ("loop-executor", "Event loop and executor phases", "runtime", "Historical loop10.6s. Numeric queue/handoff/service/threadCPU/completion phases implemented; remaining normal synchronous calls and native busy/disk partition unresolved."),
    ("transcript", "Transcript/wire durability and drain", "acp", "Integrated fixture retains exactly9600ordered events;300drains plus10tool-free cold starts, zero failures/slow/timeouts. Real-provider/network and unexercised wire/read paths remain open."),
    ("control", "Progress/dispatch/reconciliation amplification", "progress", "Compact candidate/status fixes merged; normal enqueue synchronous checkpoints and leaf/control-path coverage remain open."),
    ("placement", "Fleet/provider/selection", "fleet", "MCP bootstrap18.5s, placementp9527.1s, dispatchp9517.5s observed. Cache/catalog fixes do not establish all placement/probe acceptance."),
    ("pr-external", "PR supervision and external dependencies", "pr_supervisor", "GitHub snapshot25.7s, refreshp9514.8s, effects-dispatchp9535.6s(13samples). Separate network latency, local hydration and repeat-failure behavior."),
    ("pages", "Pages and API responsiveness", "http", "All115route groups and6historical shell families retained below;59routep95>=500ms include small samples. Normal83SSE observations excluded from ordinary request SLO."),
    ("backups", "Backup status and manifest failures", "backup", "Status53.7s and duplicate compressed archive scans; PR483 merged83c4ba7. Existing sibling owns large-manifest integrity/scalability and rollout acceptance; no competing backup edits."),
    ("maintenance", "Workspace maintenance and leases", "workspace", "Historical GC11.3s, renew6.7s, provision6.5s, maintenance13.4s; current causes unverified; retain fencing."),
    ("other-leaves", "Every remaining operation family", "other", "Prompt intake, preferences, filesystem, notification, conversation, goal, health, receipt, runtime, selection and provider exact names remain explicit rows with next action; no shared-cause closure by assumption."),
    ("peers", "Peer stalls and repeated failures", "fleet", "macmini02dbcd47:reconcile92.9s/push79s; original fleet4762,PR62,sync173failures; later snapshot worsened. Other configured peer identity unknown after bounded read timeout. Peer logs/build uninspected."),
    ("observability", "Log loss, provenance and coverage", "runtime", "CLI5.64MBdropped/2396status failures; PR485 fixes UUID provenance and WARNING normalization, not all pump losses. Own16-file retained scan covers Oct8–16; unknown timestamps3200 and missing older/peer logs remain gaps."),
    ("ambiguous-upload", "Attachment timeout and receipt recovery", "operation", "Independent reportd6f9f96d correlation89e1728a-ce91-49c1-a036-5bc8bc7cfd66 operation9fce2c73-175e-4495-aca2-aafb3c5bcc2e; own report0e918df2 fullID in ledger references. Exact-hash effects exist despite unknown transport/absent receipt; never retry ambiguously."),
]


def disposition(name: str, family: str, fixture: dict | None = None) -> dict:
    cause, fix, action = FAMILIES.get(family, (
        "Shared contention/CPU/copy amplification is a hypothesis for this leaf; exact cause unconfirmed.",
        "No operation-specific fix or verified resolution claimed.",
        "Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions.",
    ))
    return {
        "cause_classification": "mixed_confirmed_and_unverified" if family in FAMILIES else "hypothesis",
        "cause": cause, "fix_or_existing_owner": fix,
        "owner_card": BACKUP if family == "backup" or "backups" in name else CARD,
        "linked_cards": [DATABASE] + ([BACKUP] if "backups" in name else []),
        "implemented": "See fix field; not an assertion this exact finding is resolved",
        "merged": ["https://github.com/petersky/pa/pull/484", "https://github.com/petersky/pa/pull/485"] if family in FAMILIES else [],
        "deployed": "unverified; running1.5.6 at08:55Z; installed/published1.5.8 predates repairs",
        "retested": fixture or "No exact-path integrated measurement yet; open",
        "resolution": "open", "next_actor": "existing backup owner" if family == "backup" or "backups" in name else "this card/session",
        "next_action": action,
    }


def build(independent: dict, integrated: dict) -> dict:
    baseline = {x["operation"]: x for x in integrated["baseline_operations"]}
    independent_by_name = {x["operation"]: x for x in independent["all_logged_slow_operations"]}
    independent_names = set(independent_by_name)
    assert len(independent_names) == 105 and independent_names <= baseline.keys()
    rows = []
    for name, b in baseline.items():
        counters = b.get("fixture_counter_delta")
        verification = ({"counter_delta": counters,
                         "latency_ms": b.get("fixture_explicit_latency"),
                         "source": "attachment:e2d70c9f37544c02a4823bc0f1179fe3",
                         "limitation": "Only this fixture/path; not deployed/peer/whole-family acceptance"}
                        if counters and counters.get("submitted", 0) else None)
        rows.append({"id": "slow:" + name, "kind": "slow_log", "finding": name,
                     "family": b["family"], "independent_105": name in independent_names,
                     "independent_measurement": independent_by_name.get(name),
                     "measurement": b, **disposition(name, b["family"], verification)})
    for op in independent["runtime"]["all_operations"]:
        name = op["operation"]
        rows.append({"id": "runtime:" + name, "kind": "runtime_operation", "finding": name,
                     "family": name.split(".")[0], "measurement": op,
                     **disposition(name, name.split(".")[0])})
    for route in independent["runtime"]["all_routes"]:
        name = route["route"]
        rows.append({"id": "route:" + name, "kind": "http_route", "finding": name,
                     "family": "backup" if "backups" in name else "http", "measurement": route,
                     **disposition(name, "backup" if "backups" in name else "http")})
    for section, kind in [("page_shell_logged_ms", "page_shell"), ("startup_phase_logged_ms", "startup_phase")]:
        for name, stats in independent[section].items():
            rows.append({"id": kind + ":" + name, "kind": kind, "finding": name,
                         "family": kind, "measurement": stats, **disposition(name, kind)})
    for key, title, family, evidence in CONCERNS:
        rows.append({"id": "concern:" + key, "kind": "concern", "finding": title,
                     "family": family, "measurement": {"evidence": evidence}, **disposition(title, family)})
    assert len({r["id"] for r in rows}) == len(rows)
    return {"contract": "pa.performance-coverage/v1", "card_id": CARD,
            "as_of": "2026-10-16", "completion": "open; all findings require deployed acceptance or an evidence-backed final disposition",
            "source_head": HEAD, "merge_commit": MERGE,
            "input_scope": independent["coverage"],
            "own_retained_scope": {"files": 16, "bytes_at_open": 86440227, "physical_lines": 570983,
                                   "first": "2026-10-08T03:25:05.396667Z", "last": "2026-10-16T07:10:18.520347Z",
                                   "unknown_timestamp_lines": 3200, "raw_slow_emissions": 18115,
                                   "names": 110, "families": 22},
            "interpretation": ["Slow-log distributions are threshold-selected, may duplicate cross-sink emissions and mix versions.",
                               "Runtime percentiles are bounded windows; lifetime maxima/counters have a different scope.",
                               "Normal SSE durations excluded; small route samples retained rather than treated as stable percentiles.",
                               "Fixture zero violations does not establish deployed or fleet-wide acceptance.",
                               "Shared fixes cannot close unmeasured leaves; every row remains open until exact-path evidence and acceptance."],
            "references": ["attachment:ca676cf3a65c4c439e5ca8937e30ec38", "attachment:3789c5f965d24edba16d8e9aea164e0f",
                           "attachment:861e8a6e7ea14651918f84a557851d5f", "attachment:e2d70c9f37544c02a4823bc0f1179fe3",
                           "report:0e918df2-4b5c-44f1-aff2-ad7a98175744", "report:8ef59858-fd0d-4dcf-85c6-867c09bd2b38"],
            "rows": rows}


def markdown(data: dict) -> str:
    lines = ["# Systemic performance coverage", "", "This is an open work ledger, not a claim that all PA performance issues are fixed.", "",
             "Every finding has a cause assessment, repair/owner, exact measurement status, rollout state, residual and next actor in [the JSON matrix](performance-coverage.json). All rows remain open pending sufficient verification and acceptance.", "",
             "The original independent attachments contain 233,892 redacted records (2026-10-15T05:48:13Z–2026-10-16T05:48:23Z), 105 slow names, 181 runtime operations and 115 route groups. The additional retained-file inventory covers 16 files, 86,440,227 source bytes and 570,983 physical lines, Oct8–16, with 110 slow names in22families. Cross-sink duplicates, dropped logs, 3,200 unattributable lines, uninspected peer logs and older retention gaps prevent an exhaustive fleet claim.", "",
             "PR484 and PR485 are merged. At exact merged-tree761a4c6, 100push/100reconcile/300drain mixed iterations retained9600events with zero slow warnings/failures/timeouts; ten tool-free ACP fixture cold starts passed. Running1.5.6 is not verified to contain these repairs. Real provider, peer, unmeasured leaf/route and physical-write acceptance remains open.", "",
             "## Concern dispositions", "", "| Finding | Evidence and remaining work | Owner |", "|---|---|---|"]
    for r in data["rows"]:
        if r["kind"] == "concern":
            lines.append(f'| {r["finding"]} | {r["measurement"]["evidence"]} | {r["owner_card"]} |')
    lines += ["", "## Exact observed slow operations", "", "Counts are raw retained slow emissions, not distinct incidents. Milliseconds; p50/p95/p99/max are threshold-selected. Fixture evidence refers only to the named operation in the declared integrated scenario.", "", "| Operation | Count | p50 | p95 | p99 | Max | Exact integrated fixture |", "|---|---:|---:|---:|---:|---:|---|"]
    for r in data["rows"]:
        if r["kind"] != "slow_log":
            continue
        b = r["measurement"]
        check = r["retested"]
        coverage = (f'{check["counter_delta"].get("submitted", 0)} calls; {check["counter_delta"].get("slow_calls", "unknown")} slow; full metrics in JSON' if isinstance(check, dict) else "Open: exact path not exercised")
        lines.append(f'| `{r["finding"]}` | {b["count"]} | {b["p50_ms"]} | {b["p95_ms"]} | {b["p99_ms"]} | {b["max_ms"]} | {coverage} |')
    lines += ["", "## Runtime and routes", "", "All 181 operation records and 115 route groups are retained individually in the JSON, including queue/lock samples and payload bytes where present. These dated runtime windows are not contemporaneous production retests. The 83 normal SSE observations excluded by the source remain excluded.", "",
              "## Next verification", "", "Investigate normal enqueue/checkpoint behavior and remaining control-plane/leaf paths in isolated mixed fixtures; fix confirmed synchronous work. Separate external dependency service time from local overhead. Coordinate backup acceptance on0331d21a and database/API acceptance on89957d94. Obtain supported peer evidence and repair bounded repeated-failure behavior. Address mutation timeout/receipt observability without ambiguous replay. Publish updated measured outcomes and exact rollout identity before closing any deployed acceptance gap.", ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("independent", type=Path)
    parser.add_argument("integrated", type=Path)
    parser.add_argument("--output", type=Path, default=Path("docs/performance-coverage.json"))
    args = parser.parse_args()
    raw = args.independent.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "ba99e696a5eb82f440541d56b6801b7b61a1478cb22db4b4550b1e15e6ddbe31", "independent attachment hash mismatch"
    data = build(json.loads(raw), json.loads(args.integrated.read_text()))
    args.output.write_text(json.dumps(data, indent=2) + "\n")
    args.output.with_suffix(".md").write_text(markdown(data))
    print(json.dumps({"rows": len(data["rows"]), "kinds": {kind: sum(r["kind"] == kind for r in data["rows"]) for kind in sorted({r["kind"] for r in data["rows"]})}}))


if __name__ == "__main__":
    main()
