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
    ("incoming-fork-fence", "Incoming fork publication and summary writes", "sync", "Expanded25-request mixed fixture exposed one unhandled DagIndexStaleError during summary publication, despite zero measured latency violations. Confirmed incoming merge advanced ref before application primary admission; repair routes prepared immutable merge through projection/CAS publisher and offloads summary work with fresh-input/manual-edit fences. Report19c44107-99dc-4b64-92ea-fda8d22a3418; regression/retest status below."),
    ("large-message-copy", "Quadratic complete-message compaction", "acp", "Expanded100-iteration fixture exposed transcript message-page tails as a single message grew to3200chunks/2.56million characters. Repeated prefix concatenation materialized about4.1billion characters. Linear grouping joins once while preserving replacement snapshots, source sequence spans, interleaved tools and all durable input chunks; exact reconstruction and copy-volume regression passed."),
    ("startup-classification", "Startup classification and same-request recovery", "selection", "Report e2bed017-d546-49f5-910e-3d8326d80e40: start classified as research, same request recovered after metadata correction. The workaround is not a repair or new authorization. Preserve request/receipt identity and user model/permission defaults; root classification and regression coverage remain open."),
    ("creation-version", "Known disabled summary metadata races card version", "sqlite", "PR486 CI38883010060 exposed a disabled-summary metadata write about10ms after a caller read the card version, rejecting its following execution-preference edit. Correction settles known unconfigured metadata in the initial canonical creation event and exact receipt. Configured/manual summary behavior and strict stale-version rejection remain intact; PR486 merged687436f after all5checks passed; optional-service absence remains compatible with degraded local operations (9targeted tests)."),
    ("completion-scope", "Integration-only automatic completion exceeds card scope", "operation", "Supervisor marked this expanded performance card Done after PR484, PR485, PR486 and PR487 despite open acceptance/peer/coverage findings; card restored Active four times. Report861aef1f-3d04-461d-8594-fc5b4260ccf2 revision4 preserves evidence. Integration is not whole-PA acceptance; completion-policy/requirement coordination remains open, with no invented acceptance receipt or weakened gate."),
    ("normal-enqueue", "Duplicate durable human enqueue checkpoint", "progress", "100 real paused human enqueue calls produced200checkpoints (p95enqueue13.882ms/max18.055ms). Removing the redundant second checkpoint yields100checkpoints, p959.980ms/max11.701ms, with all100prompts durable. Existing queue/recovery/acceptance suites:87passed. PR487 merged8fcc3ce after supervised stable-green head and5/5head+mainCI. Ordinary synchronous checkpoint cost remains; no claim of whole-PA acceptance."),
    ("startup-schema-cpu", "Full Kernel schema warm-up blocks completion delivery", "runtime", "Disposable full boot: OpenAPI2919.984ms wall/2878.777ms CPU on loop; concurrent sync-ref service1.57ms/delivery3015.695ms. Exact offload alternative preserves readiness paths but still records schema1443.870ms and ref520.821ms warnings. No source readiness repair committed; real full-boot and GIL/CPU limits remain open. Reportd2db90ed-30f7-4cca-ae3a-af18d4fa056a."),
    ("pr-inventory-hydration", "PR metrics and health hydrate proofs to count lifecycle", "pr_supervisor", "100retained watches/30metrics calls hydrate3000records/193086000state-policy bytes; baselinep9594.061ms. SQLaggregate replacement:0proof hydrations, same counts, clean sequentialp958.667ms.99new/existing PR tests plus5final inventory contracts passed. Final mixed run still2worker warnings/four measured violations; no blanket root-cause or deployment closure."),
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


def build(independent: dict, integrated: dict, followup: dict | None = None) -> dict:
    baseline = {x["operation"]: x for x in integrated["baseline_operations"]}
    independent_by_name = {x["operation"]: x for x in independent["all_logged_slow_operations"]}
    independent_names = set(independent_by_name)
    assert len(independent_names) == 105 and independent_names <= baseline.keys()
    rows = []
    measured = integrated["integrated_fixture"]
    def fixture_evidence(name: str) -> dict | None:
        counters = measured.get("operation_counter_deltas", {}).get(name)
        if not counters or not counters.get("submitted", 0):
            return None
        return {"counter_delta": counters, "latency_ms": measured.get("latency", {}).get(name),
                "source": "attachment:e2d70c9f37544c02a4823bc0f1179fe3",
                "limitation": "Only this fixture/path; not deployed/peer/whole-family acceptance"}
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
                     **disposition(name, name.split(".")[0], fixture_evidence(name))})
    for route in independent["runtime"]["all_routes"]:
        name = route["route"]
        exact_requests = {path: stats for path, stats in measured.get("latency", {}).items()
                          if path.split("?", 1)[0] == name}
        verification = ({"latency_by_exact_request": exact_requests,
                         "response_bytes": {path: measured.get("response_bytes", {}).get(path) for path in exact_requests},
                         "source": "attachment:e2d70c9f37544c02a4823bc0f1179fe3",
                         "limitation": "Only listed query variants and fixture data; no deployed acceptance"}
                        if exact_requests else None)
        rows.append({"id": "route:" + name, "kind": "http_route", "finding": name,
                     "family": "backup" if "backups" in name else "http", "measurement": route,
                     **disposition(name, "backup" if "backups" in name else "http", verification)})
    for section, kind in [("page_shell_logged_ms", "page_shell"), ("startup_phase_logged_ms", "startup_phase")]:
        for name, stats in independent[section].items():
            rows.append({"id": kind + ":" + name, "kind": kind, "finding": name,
                         "family": kind, "measurement": stats, **disposition(name, kind)})
    for key, title, family, evidence in CONCERNS:
        rows.append({"id": "concern:" + key, "kind": "concern", "finding": title,
                     "family": family, "measurement": {"evidence": evidence}, **disposition(title, family)})
    if followup:
        baseline_runtime_names = {r["finding"] for r in rows if r["kind"] == "runtime_operation"}
        for name, counters in followup.get("operation_counter_deltas", {}).items():
            if name not in baseline_runtime_names and counters.get("submitted", 0):
                rows.append({"id": "runtime-followup:" + name, "kind": "runtime_operation_followup",
                             "finding": name, "family": name.split(".")[0],
                             "measurement": {"counter_delta": counters,
                                             "phases": followup.get("runtime", {}).get("operations", {}).get(name),
                                             "scope": "new followup operation; phase window may include boot, counter delta covers measured window"},
                             **disposition(name, name.split(".")[0])})
        for row in rows:
            name = row["finding"]
            exact = followup.get("operation_counter_deltas", {}).get(name)
            routes = {path: stats for path, stats in followup.get("latency", {}).items()
                      if path.split("?", 1)[0] == name}
            if (exact and exact.get("submitted", 0)) or routes:
                row["followup_verification"] = {
                    "artifact": "benchmarks/systemic-inventory-v4.json",
                    "counter_delta": exact, "latency_by_exact_request": routes,
                    "response_bytes": {path: followup.get("response_bytes", {}).get(path) for path in routes},
                    "scope": "isolated version4 mixed fixture; source patch verification, deployment unverified",
                    "scenario_warnings": followup.get("warnings"),
                    "scenario_threshold_violations": followup.get("threshold_violations"),
                }
        for row in rows:
            if row["id"] == "concern:ambiguous-upload":
                row["implemented"] = "Canonical attachment event carries request key/fingerprint/exact result; MCP exposes optional stable key; publication offloaded. Future operations only, no fabricated historical receipt."
                row["retested"] = "44 attachment/materialization/canonical-receipt regressions passed; replay at capacity, changed-byte conflict, lost postcommit response, rebuild, unrelated-API responsiveness."
            if row["id"] == "concern:incoming-fork-fence":
                row["implemented"] = "Prepared immutable merge uses existing application publisher; summary reads/writes offloaded with atomic fresh-input/attempt/manual-source revalidation."
                row["retested"] = "Deterministic primary-writer/CAS-retry/both-parent regression and summary manual-edit-after-worker-wait regression passed; expanded workload in followup artifact."
            if row["id"] == "concern:large-message-copy":
                row["implemented"] = "Linear chunk grouping with one text join per completed group; only derived browser pages change, durable events untouched."
                row["retested"] = "8 exact historical reconstruction tests passed, including3200chunk/2.56million-character copy-volume and immutable-source assertions."
            if row["id"] == "concern:creation-version":
                row["implemented"] = "Known unconfigured summary state is part of the original canonical creation event and exact receipt; optional configuration failures retain asynchronous ownership. Form writes use bounded executor."
                row["retested"] = "7 creation/selection tests passed with strict stale-version rejection unchanged, exact replay/rebuild and delayed disabled task. Broader62tests+9subtests passed; initial new fixture compared equivalent Z/+00:00 strings, corrected to exact datetime equality."
            if row["id"] in {"concern:ambiguous-upload", "concern:incoming-fork-fence", "concern:large-message-copy", "concern:creation-version"}:
                row["merged"] = ["https://github.com/petersky/pa/pull/486"]
                row["integration_evidence"] = {"head":"682561f90ed02f2e86af64c7d699ab898ff76d2d", "merge":"687436f58a7079649a24c42bb521f635664873b4", "tree":"1d86c251b9004ef7e157be577a8e9169635c9d7e", "watch":"bd009a71-422f-4b00-8ae0-1230fdc31012", "head_ci":38885531633, "checks_success":5}
            exact_causes = {
                "concern:incoming-fork-fence": "Confirmed incoming fork published ref before primary admission; a concurrent canonical writer then saw a stale fenced parent. Deterministic CAS-retry regression verifies the corrected publication boundary.",
                "concern:large-message-copy": "Confirmed repeated text prefix concatenation:3199copies/4097279200characters for3200chunks. Linear grouping preserves exact output with1copy/2560000characters.",
                "concern:creation-version": "Confirmed optional disabled-summary metadata was a second canonical write after creation, advancing the caller's card version. Initial-event metadata removes that write without weakening CAS. Missing optional-service registration is separately covered by degraded-kernel regressions.",
                "concern:ambiguous-upload": "Confirmed attachment canonical events omitted request-key/fingerprint/result recovery metadata. Historical transport timeout and missing recovery-key exposure remain separate unverified causes; future receipt repair does not invent historical receipts.",
                "concern:normal-enqueue": "Confirmed source and same-host measurement: human activity and cursor are already included in the first checkpoint; synchronous transcript flush schedules the same events without changing recoverable state. Second checkpoint duplicates that durable write.",
            }
            if row["id"] in exact_causes:
                row["cause"] = exact_causes[row["id"]]
                row["cause_classification"] = "confirmed_source_cause_with_open_deployed_acceptance"
            if row["id"] == "concern:normal-enqueue":
                row["implemented"] = "PR487 removes only the redundant second checkpoint; first durable save, queue ordering, human activity, cursor and transcript flush retained."
                row["merged"] = ["https://github.com/petersky/pa/pull/487"]
                row["integration_evidence"] = {"head":"a5dd220fb32834cb517fccb2ff594d7ad6b686a3", "merge":"8fcc3ce4f48b1e27e64e5e69b491cbf2a4e62c92", "tree":"b6e2857bd712f7ea32ed47d9000555165236bca9", "watch":"79fc6c48-c28a-4d46-b105-050220d7bb8f", "head_ci":38887025596, "main_ci":38887997502, "head_main_checks_success":5}
                row["retested"] = "87 existing queue/recovery/durable-admission tests passed. Identical100enqueue comparison retains all100prompts and halves checkpoints200to100; docs/benchmarks/prompt-enqueue-checkpoints.json."
                row["next_action"] = "Integrated source verified; measure normal admission under larger mixed queues and installed workload."
            if row["id"] == "concern:startup-schema-cpu":
                row.update(cause_classification="confirmed_disposable_startup_cpu_cause", cause="Timer around full-schema generation confirms loop-thread CPU and simultaneous completion delivery delay. It does not attribute every live historical loop stall.", implemented="None: diagnostic offload alternative only; still slow. No readiness gate, OpenAPI contract or deadline changed.", merged=[], retested="One isolated before/alternative full boot, required paths present in both; source version and all numeric residuals on card/reportd2db90ed.", next_action="Reduce or isolate schema CPU while preserving readiness/admission and verify full cold boot; retain remaining GIL/completion delays.")
            if row["id"] == "concern:pr-inventory-hydration":
                row.update(cause_classification="confirmed_copy_amplification", cause="metrics() and authority_health() decoded all retained watch policy/state to calculate counts, ownership and maximum active fence; source and3000row/193MB probe confirm unnecessary hydration.", implemented="Local source patch uses aggregate metadata SQL, preserves lifecycle/owner/fence predicates and public fields, adds numeric SQL observations; integration pending.", merged=[], retested="99new/existing PR supervisor tests then5final inventory contracts pass; per-lifecycle/owner/fence parity, empty catalog, no hydration, SQL row observations; clean30call comparison and100mixed iterations retained with residuals.", next_action="Complete supervised integration and exact-head checks; exercise nonempty active-watch API and real external poll/effect paths, then installed acceptance.")
            if row["id"] == "concern:completion-scope":
                row["next_actor"] = "this session and PA completion policy/requirement owner"
                row["next_action"] = "Keep expanded card open; coordinate a completion contract covering all residual acceptance and owner gates before final Done. Do not invent independent acceptance or clear existing requirements."
    assert len({r["id"] for r in rows}) == len(rows)
    return {"contract": "pa.performance-coverage/v1", "card_id": CARD,
            "as_of": "2026-10-16", "completion": "open; all findings require deployed acceptance or an evidence-backed final disposition",
            "baseline_source_head": HEAD, "baseline_merge_commit": MERGE,
            "followup_source_evidence": ({"stage": followup.get("evidence_stage"),
                                          "supplemental": followup.get("supplemental_source_attestation"),
                                          "module_sha256": followup.get("source_module_sha256")}
                                         if followup else None),
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
                           "report:0e918df2-4b5c-44f1-aff2-ad7a98175744", "report:8ef59858-fd0d-4dcf-85c6-867c09bd2b38",
                           "report:fb45137a-c958-4356-93fc-cbf9ecb9ad49", "report:e2bed017-d546-49f5-910e-3d8326d80e40",
                           "report:d2db90ed-30f7-4cca-ae3a-af18d4fa056a", "report:f6391c9f-68ad-41ca-8035-60d9ab2e0b28",
                           "report:861aef1f-3d04-461d-8594-fc5b4260ccf2", "report:d09ac79d-3df3-49e9-a426-8e0b3e156355",
                           "report:19c44107-99dc-4b64-92ea-fda8d22a3418", "report:e58d4d65-5332-4dac-8de9-5ae244739ac5"],
            "followup_artifact": "benchmarks/systemic-inventory-v4.json" if followup else None,
            "rows": rows}


def markdown(data: dict) -> str:
    lines = ["# Systemic performance coverage", "", "This is an open work ledger, not a claim that all PA performance issues are fixed.", "",
             "Every finding has a cause assessment, repair/owner, exact measurement status, rollout state, residual and next actor in [the JSON matrix](performance-coverage.json). All rows remain open pending sufficient verification and acceptance.", "",
             "The original independent attachments contain 233,892 redacted records (2026-10-15T05:48:13Z–2026-10-16T05:48:23Z), 105 slow names, 181 runtime operations and 115 route groups. The additional retained-file inventory covers 16 files, 86,440,227 source bytes and 570,983 physical lines, Oct8–16, with 110 slow names in22families. Cross-sink duplicates, dropped logs, 3,200 unattributable lines, uninspected peer logs and older retention gaps prevent an exhaustive fleet claim.", "",
             "PR484 and PR485 are merged. At exact merged-tree761a4c6, 100push/100reconcile/300drain mixed iterations retained9600events with zero slow warnings/failures/timeouts; ten tool-free ACP fixture cold starts passed. Running1.5.6 is not verified to contain these repairs. Real provider, peer, unmeasured leaf/route and physical-write acceptance remains open.", "",
             "PR486 is merged as687436f from watched stable-green682561f with all5head checks successful. The expanded100-iteration source-matched scenario has0worker warnings/failures/timeouts,9600ordered events, but2HTTP500ms violations (message history518.524ms;PullRequests675.397ms) and loopmax508.158ms. This remains failed performance acceptance. All earlier outliers remain in [the numeric artifact](benchmarks/systemic-inventory-v4.json). PR487 enqueue reduction is also integrated, exactmerge8fcc3ce with5/5head+mainCI; original CI failures/retry retained. These earlier scenario results are not superseded by source integration alone.", "",
             "Latest PR-inventory source patch:99new/existing supervisor tests plus5final inventory contracts pass.100retained-watch/30metrics measurement reduces proof hydration3000rows/193MB to0 and p9594.061to8.667ms. The same100-iteration mixed scenario retains9600events, but2worker warnings and4measured violations remain (drain507.378ms;push548.788ms;reconcile534.643ms;message597.144ms), loopmax180.803ms. This source patch is pending integration; no deployed acceptance. Nonempty active-watch API and real external polling remain explicit gaps.", "",
             "## Concern dispositions", "", "| Finding | Evidence and remaining work | Owner |", "|---|---|---|"]
    for r in data["rows"]:
        if r["kind"] == "concern":
            lines.append(f'| {r["finding"]} | {r["measurement"]["evidence"]} | {r["owner_card"]} |')
    lines += ["", "## Exact observed slow operations", "", "Counts are raw retained slow emissions, not distinct incidents. Milliseconds; p50/p95/p99/max are threshold-selected. Fixture evidence refers only to the named operation in the declared integrated scenario.", "", "| Operation | Count | p50 | p95 | p99 | Max | Earlier PR485 fixture; latest follow-up in JSON |", "|---|---:|---:|---:|---:|---:|---|"]
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
    parser.add_argument("--followup", type=Path,
                        help="Additional isolated mixed-workload numeric results; never implies deployed acceptance")
    args = parser.parse_args()
    raw = args.independent.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "ba99e696a5eb82f440541d56b6801b7b61a1478cb22db4b4550b1e15e6ddbe31", "independent attachment hash mismatch"
    followup = json.loads(args.followup.read_text()) if args.followup else None
    data = build(json.loads(raw), json.loads(args.integrated.read_text()), followup)
    args.output.write_text(json.dumps(data, indent=2) + "\n")
    args.output.with_suffix(".md").write_text(markdown(data))
    print(json.dumps({"rows": len(data["rows"]), "kinds": {kind: sum(r["kind"] == kind for r in data["rows"]) for kind in sorted({r["kind"] for r in data["rows"]})}}))


if __name__ == "__main__":
    main()
