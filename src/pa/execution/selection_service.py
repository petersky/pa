"""Shared orchestration for local creation, fleet admission and read-only previews."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

from pa.acp.configuration import SessionConfigurationRequest
from pa.core.preferences import get_preferences_store
from pa.execution.selection import (
    ExecutionCandidate,
    ExecutionPreferences,
    Preference,
    SelectionConstraints,
    SelectionError,
    TaskAssessment,
    assess_task,
    digest,
    legacy_preferences,
    resolve_selection,
    validate_reuse,
)
from pa.execution.selection_catalog import candidates_from_advertisement
from pa.execution.selection_store import SelectionStore


def preference_layers(
    settings,
    *,
    principal: str | None,
    surface: str,
    card=None,
    project_config=None,
    overrides=None,
    legacy=None,
) -> list[tuple[str, ExecutionPreferences]]:
    overrides = overrides or ExecutionPreferences()
    legacy = legacy or ExecutionPreferences()
    from pa.execution.selection import FIELDS

    for name in FIELDS:
        modern, old = getattr(overrides, name), getattr(legacy, name)
        if (
            modern.intent == "required"
            and old.intent == "required"
            and modern.value != old.value
        ):
            raise SelectionError(
                "conflicting_explicit_selectors",
                f"Explicit {name} conflicts with its legacy selector; supply one consistent value.",
            )
    layers = [("dispatch", overrides), ("explicit_legacy", legacy)]
    if card is not None:
        layers.append(("card", card.execution_preferences))
    project_config = project_config or {}
    layers.append(
        (
            "project",
            ExecutionPreferences.model_validate(
                project_config.get("execution_preferences") or {}
            ),
        )
    )
    project_provider = project_config.get("agent_provider") or project_config.get(
        "provider"
    )
    if project_provider:
        layers.append(("project_legacy", legacy_preferences(provider=project_provider)))
    user_id = principal[5:] if principal and principal.startswith("user:") else None
    for name, scope in ([("user", user_id)] if user_id else []) + [
        ("installation_surface", None)
    ]:
        prefs = get_preferences_store(settings.data_dir, user_id=scope).load()
        specific = prefs.agent_surfaces.get(surface)
        if specific:
            layers.append((name, specific.execution_preferences))
            layers.append(
                (
                    name + "_legacy",
                    legacy_preferences(
                        provider=specific.provider,
                        model_id=specific.model_id,
                        model_provider=specific.model_provider,
                        effort=specific.effort,
                        config=specific.config,
                    ),
                )
            )
        if prefs.agent_provider:
            layers.append(
                (
                    name + "_provider",
                    ExecutionPreferences(
                        harness=Preference(
                            intent="preferred", value=prefs.agent_provider
                        )
                    ),
                )
            )
    if getattr(settings, "agent_provider", None):
        layers.append(
            (
                "installation_provider",
                ExecutionPreferences(
                    harness=Preference(
                        intent="preferred", value=settings.agent_provider
                    )
                ),
            )
        )
    return layers


def status_candidates(
    instance_id: str, statuses: list[dict], *, observed_at=None, freshness="fresh"
) -> list[ExecutionCandidate]:
    now = datetime.now(UTC)
    if isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at)
    observed_at = observed_at or now
    result = []
    for status in statuses:
        harness = str(status.get("id") or "")
        if not harness:
            continue
        for raw in status.get("execution_candidates") or []:
            try:
                scoped = ExecutionCandidate.model_validate(raw).model_copy(
                    update={"instance_id": instance_id}
                )
                result.append(scoped)
            except ValueError:
                # Mixed-version peers cannot advertise usable tuples with an
                # unknown schema. Their legacy default remains separate.
                continue
        ready = status.get("available") and status.get("auth_state") == "authenticated"
        readiness = (
            "ready"
            if ready
            else "unavailable"
            if status.get("auth_state")
            in {"signed_out", "probe_failed", "not_configured", "timed_out"}
            else "unknown"
        )
        probe = status.get("last_probe") or {}
        evidence = status.get("auth_evidence") or []
        if probe.get("ok") is False:
            readiness = "unavailable"
        elif (
            ready
            and evidence
            and set(evidence) <= {"configured_credential", "no_auth_required"}
            and probe.get("ok") is not True
        ):
            readiness = "unknown"
        meta = status.get("meta") or {}
        catalogs = status.get("execution_catalogs") or [
            dict(
                meta.get("options") or {},
                models=status.get("models")
                or meta.get("models")
                or (meta.get("options") or {}).get("models"),
                model_provider=meta.get("model_provider"),
                source="adapter_status",
            )
        ]
        for catalog in catalogs:
            catalog_readiness = (
                "ready" if catalog.get("source") == "live_acp_session" else readiness
            )
            result.extend(
                candidates_from_advertisement(
                    instance_id=instance_id,
                    harness=harness,
                    advertisement=catalog,
                    readiness=catalog_readiness,
                    observed_at=observed_at,
                    source=catalog.get("source", "adapter_status"),
                    freshness=freshness,
                    connection=catalog.get("connection", "default"),
                    model_provider=catalog.get("model_provider"),
                    health_evidence=status.get("auth_evidence") or [],
                )
            )
    return list({c.key: c for c in result}.values())


class SelectionService:
    def __init__(self, settings, domain_store, manager=None):
        self.settings = settings
        self.domain_store = domain_store
        self.manager = manager
        self.store = SelectionStore(settings.data_dir)
        self._refresh_lock = asyncio.Lock()
        self.catalog_generation = 0
        self._last_forced_refresh = float("-inf")

    async def local_catalog(
        self, *, refresh=False, force=False
    ) -> list[ExecutionCandidate]:
        generation = self.catalog_generation
        scope = self.settings.instance_id
        rows, stamp = await asyncio.to_thread(self.store.catalog, scope)
        age = (datetime.now(UTC) - stamp).total_seconds() if stamp else None
        if force or (refresh and (age is None or age > 60)):
            async with self._refresh_lock:
                rows, stamp = await asyncio.to_thread(self.store.catalog, scope)
                age = (datetime.now(UTC) - stamp).total_seconds() if stamp else None
                force_due = force and time.monotonic() - self._last_forced_refresh >= 3
                if generation == self.catalog_generation and (
                    force_due if force else age is None or age > 60
                ):
                    if force:
                        self._last_forced_refresh = time.monotonic()
                    from pa.acp.providers.resolve import list_provider_summaries_bounded

                    statuses = await list_provider_summaries_bounded(
                        self.settings.data_dir, manager=self.manager
                    )
                    # An authenticated adapter may expose models only at
                    # session/new. Discover them without submitting a prompt.
                    from pa.execution.selection_catalog import discover_missing_catalogs

                    statuses = await discover_missing_catalogs(statuses, self.settings)
                    candidates = status_candidates(scope, statuses)
                    rows = [c.model_dump(mode="json") for c in candidates]
                    await asyncio.to_thread(self.store.save_catalog, scope, rows)
                    stamp = datetime.now(UTC)
                    self.catalog_generation += 1
        rows = await asyncio.to_thread(self._current_connection_rows, rows)
        candidates = [ExecutionCandidate.model_validate(row) for row in rows]
        now = datetime.now(UTC)
        candidates = [
            c.model_copy(update={"freshness": "stale"})
            if not stamp
            or (now - stamp).total_seconds() > 300
            or (now - c.observed_at).total_seconds() > 300
            else c
            for c in candidates
        ]
        return candidates

    def _current_connection_rows(self, rows):
        from pa.execution.selection_connections import connection_revision

        revisions = {}
        for profile in self.store.connections():
            if profile.enabled:
                try:
                    revisions[profile.id] = connection_revision(
                        profile, self.settings.data_dir
                    )
                except SelectionError:
                    pass
        return [
            row
            for row in rows
            if not row.get("connection_revision")
            or row["connection_revision"] == revisions.get(row.get("connection"))
        ]

    async def refresh_connection(self, connection_id):
        from pa.execution.selection_connections import discover_connection

        profile = next(
            (
                p
                for p in await asyncio.to_thread(self.store.connections)
                if p.id == connection_id
            ),
            None,
        )
        if not profile or not profile.enabled:
            raise SelectionError(
                "connection_not_found",
                "Enabled configured connection not found on this instance",
            )
        async with self._refresh_lock:
            cached, stamp = await asyncio.to_thread(
                self.store.catalog, "connection:" + profile.id
            )
            valid = await asyncio.to_thread(self._current_connection_rows, cached)
            if (
                len(valid) != len(cached)
                or not stamp
                or (datetime.now(UTC) - stamp).total_seconds() > 60
            ):
                found = await discover_connection(profile, self.settings.data_dir)
                cached = [
                    c.model_copy(
                        update={"instance_id": self.settings.instance_id}
                    ).model_dump(mode="json")
                    for c in found
                ]
                await asyncio.to_thread(
                    self.store.save_catalog, "connection:" + profile.id, cached
                )
                local, _ = await asyncio.to_thread(
                    self.store.catalog, self.settings.instance_id
                )
                await asyncio.to_thread(
                    self.store.save_catalog,
                    self.settings.instance_id,
                    [c for c in local if c["connection"] != profile.id] + cached,
                )
        return cached

    def _project_config(self, card, supplied):
        # A caller may suggest defaults for a standalone job, but cannot replace
        # the durable card's project policy by passing another project's config.
        if card and getattr(card, "project_id", None):
            project = self.domain_store.get_project(
                card.project_id, realm_id=card.realm_id
            )
            if not project:
                raise SelectionError(
                    "selection_project_unavailable",
                    "The card's project policy is unavailable. Sync that project before admission.",
                )
            return project.tool_config
        return supplied

    def layers(
        self,
        *,
        principal,
        surface,
        card=None,
        project_config=None,
        overrides=None,
        legacy=None,
    ):
        return preference_layers(
            self.settings,
            principal=principal,
            surface=surface,
            card=card,
            project_config=self._project_config(card, project_config),
            overrides=overrides,
            legacy=legacy,
        )

    def revalidate_attempt(self, receipt, *, realm, principal, surface):
        from pa.execution.selection import revalidate_attempt_constraints

        selected = receipt["selected"]
        if selected.get("connection_revision"):
            current = self._current_connection_rows([selected])
            if not current:
                raise SelectionError(
                    "connection_revision_changed",
                    "The selected account/endpoint or credential changed. Explicitly start a linked attempt; the existing prompt will not be silently rerouted",
                )
        context = receipt.get("context") or {}
        card = (
            self.domain_store.get_card(context["card_id"], realm_id=realm)
            if context.get("card_id")
            else None
        )
        project = (
            self.domain_store.get_project(card.project_id, realm_id=realm)
            if card and card.project_id
            else None
        )
        project_config = project.tool_config if project else {}
        policy = self.store.policy(realm)
        layers = self.layers(
            principal=principal,
            surface=surface,
            card=card,
            project_config=project_config,
        )
        constraints = [
            policy.constraints,
            policy.defaults.hard_constraints,
            *(p.hard_constraints for _, p in layers),
            *(
                SelectionConstraints.model_validate(c)
                for c in receipt.get("constraints", [])
            ),
        ]
        if (project_config or {}).get("execution_constraints"):
            constraints.append(
                SelectionConstraints.model_validate(
                    project_config["execution_constraints"]
                )
            )
        revalidate_attempt_constraints(receipt, constraints)

    def linked_fallback(self, receipt, *, realm, key, prompt, candidate_key=None):
        validate_reuse(receipt)
        # Derived recoveries keep the original budget, never replenish it by
        # taking a new policy snapshot on each failed attempt.
        origin_id = (receipt.get("recovery") or {}).get("original_decision_id")
        if origin_id:
            receipt = self.store.decision(
                origin_id, realm, receipt["context"]["principal"]
            )
            if not receipt:
                raise SelectionError(
                    "fallback_origin_missing",
                    "Original recovery receipt is unavailable; operator action is required",
                )
        policy = self.store.policy(realm)
        if receipt["selected"]["connection"] not in policy.fallback_connections:
            raise SelectionError(
                "fallback_not_authorized",
                "Current policy does not authorize fallback on this connection. Use an explicit linked context-boundary action.",
            )
        candidate_key = candidate_key or receipt["candidate_key"]
        alternative = next(
            (
                a
                for a in receipt["alternatives"]
                if a["candidate_key"] == candidate_key and a["eligible"]
            ),
            None,
        )
        if not alternative:
            raise SelectionError(
                "fallback_not_authorized",
                "Alternative was not eligible under the original hard pins and constraints",
            )
        result = self.store.reserve_fallback(
            receipt,
            alternative,
            key=key,
            prompt_digest=digest(prompt),
            limit=min(
                policy.fallback_max_attempts, receipt["fallback"]["max_attempts"]
            ),
        )
        self.store.save_decision(result, realm, receipt["context"]["principal"])
        return result

    def resolve(
        self,
        *,
        candidates,
        principal,
        realm,
        surface,
        card=None,
        project_config=None,
        overrides=None,
        legacy=None,
        assessment=None,
        constraints=(),
        persist=False,
    ):
        project_config = self._project_config(card, project_config)
        policy = self.store.policy(realm)
        layers = self.layers(
            principal=principal,
            surface=surface,
            card=card,
            project_config=project_config,
            overrides=overrides,
            legacy=legacy,
        )
        assessment_source = (
            "dispatch_task_assessment" if assessment is not None else None
        )
        if assessment is None:
            assessment_source, assessment = next(
                ((source, p.task) for source, p in layers if p.task is not None),
                (None, None),
            )
        task = assess_task(
            card.title if card else "",
            card.body if card else "",
            role=surface,
            explicit=TaskAssessment.model_validate(assessment)
            if assessment is not None
            else None,
        )
        if assessment_source:
            task.provenance = {k: assessment_source for k in task.provenance}
        hard = [
            *constraints,
            SelectionConstraints(
                required_tools=task.tools,
                modalities=task.modalities,
                min_context_tokens=task.context_tokens or 0,
            ),
        ]
        if (project_config or {}).get("execution_constraints"):
            hard.append(
                SelectionConstraints.model_validate(
                    project_config["execution_constraints"]
                )
            )
        receipt = resolve_selection(
            layers=layers,
            candidates=candidates,
            policy=policy,
            assessment=task,
            constraints=hard,
            evidence=self.store.evidence(realm, principal or "user:local"),
        )
        receipt["context"] = {
            "realm": realm,
            "principal": principal or "user:local",
            "surface": surface,
            "policy_instance_id": self.settings.instance_id,
            "card_id": card.id if card else None,
            "card_version": card.updated_at.isoformat() if card else None,
        }
        receipt["decision_id"] = digest(
            {k: v for k, v in receipt.items() if k != "decision_id"}
        )
        if persist:
            self.store.save_decision(receipt, realm, principal or "user:local")
        return receipt

    def joint(
        self,
        placement,
        placement_request,
        candidates,
        *,
        principal,
        realm,
        surface,
        card=None,
        project_config=None,
        overrides=None,
        legacy=None,
        assessment=None,
        persist=False,
    ):
        from pa.fleet.placement import PlacementError, _evaluate

        tuples = []
        fleet_details = {}
        for host in candidates:
            envelope = host.providers
            natives = status_candidates(
                host.instance_id,
                envelope.get("value") or [],
                observed_at=envelope.get("observed_at"),
                freshness=envelope.get("state", "unknown")
                if envelope.get("state") in {"fresh", "stale"}
                else "unknown",
            )
            for native in natives:
                scoped_host = _host_for_tuple(host, native.model_dump(mode="json"))
                reasons, scores, detail = _evaluate(
                    placement_request.model_copy(
                        update={"provider": native.harness, "model_id": None}
                    ),
                    scoped_host,
                )
                fleet_details[native.key] = {
                    "reasons": reasons,
                    "detail": detail,
                    "scores": scores,
                }
                if reasons:
                    native = native.model_copy(
                        update={
                            "readiness": "unavailable",
                            "health_evidence": native.health_evidence + reasons,
                        }
                    )
                native = native.model_copy(
                    update={
                        "capacity_available": detail.get("execution_slot_available")
                    }
                )
                tuples.append(native)
        try:
            receipt = self.resolve(
                candidates=tuples,
                principal=principal,
                realm=realm,
                surface=surface,
                card=card,
                project_config=project_config,
                overrides=overrides,
                legacy=legacy,
                assessment=assessment,
            )
        except SelectionError as exc:
            for item in exc.receipt.get("alternatives", []):
                item["fleet"] = fleet_details.get(item["candidate_key"])
            # Keep the fleet admission error contract when no tuple passed fleet
            # eligibility. This is distinct from an incompatible explicit model.
            fleet_excluded = not tuples or all(
                v["reasons"] for v in fleet_details.values()
            )
            raise PlacementError(
                "no_eligible_instance" if fleet_excluded else exc.code,
                "No instance passes fleet admission for an advertised execution tuple"
                if fleet_excluded
                else str(exc),
                detail={"execution_selection": exc.receipt},
                rejected_candidates=[
                    v["detail"] | {"reasons": v["reasons"]}
                    for v in fleet_details.values()
                    if v["reasons"]
                ],
            ) from exc
        for item in receipt["alternatives"]:
            item["fleet"] = fleet_details.get(item["candidate_key"])
        # Select a compatible tuple per instance, then let fleet policy arbitrate
        # only the highest preference/policy scoring instances. All hard fleet
        # checks are evaluated again on the concrete chosen harness.
        eligible = [a for a in receipt["alternatives"] if a["eligible"]]
        best_score = max(a["score"] for a in eligible)
        by_instance = {}
        for item in sorted(eligible, key=lambda a: a["candidate_key"]):
            if item["score"] == best_score:
                by_instance.setdefault(item["selected"]["instance_id"], item)
        filtered = []
        for host in candidates:
            if host.instance_id not in by_instance:
                continue
            selection = by_instance[host.instance_id]["selected"]
            filtered.append(_host_for_tuple(host, selection))
        decision = placement.resolve(
            placement_request.model_copy(update={"provider": None, "model_id": None}),
            filtered,
        )
        selected = by_instance[decision.chosen_instance_id]
        receipt.update(
            selected=selected["selected"],
            candidate_key=selected["candidate_key"],
            tradeoffs=selected["tradeoffs"],
        )
        receipt["explanation"] += " " + decision.tie_breaking_reason
        receipt["decision_id"] = digest(
            {k: v for k, v in receipt.items() if k != "decision_id"}
        )
        if persist:
            self.store.save_decision(receipt, realm, principal or "user:local")
        decision.execution_selection = receipt
        return decision


def _host_for_tuple(host, selected):
    statuses = [
        dict(s)
        for s in host.providers.get("value", [])
        if s.get("id") == selected["harness"]
    ]
    if selected.get("connection_revision"):
        for status in statuses:
            match = next(
                (
                    c
                    for c in status.get("execution_candidates", [])
                    if c.get("connection") == selected["connection"]
                    and c.get("connection_revision") == selected["connection_revision"]
                    and c.get("readiness") == "ready"
                ),
                None,
            )
            if match:
                status.update(
                    available=True,
                    auth_state="authenticated",
                    auth_evidence=match.get("health_evidence", []),
                )
            else:
                # A default account's authentication never authenticates a named
                # account. Only this exact connection's evidence is relevant.
                status.update(auth_state="unknown", auth_evidence=[])
    return host.model_copy(update={"providers": {**host.providers, "value": statuses}})


def service_for(ctx) -> SelectionService:
    services = getattr(ctx, "services", None)
    if services is None:
        services = ctx.services = {}
    service = services.get("execution_selection")
    if service is None:
        manager = services.get("instance_agent")
        service = getattr(manager, "_selection_service", None) or SelectionService(
            ctx.settings, ctx.store, manager
        )
        if manager is not None:
            manager._selection_service = service
        services["execution_selection"] = service
    return service


def selected_configuration(
    receipt: dict,
    authority: SessionConfigurationRequest | None = None,
    *,
    native_binding: dict | None = None,
) -> SessionConfigurationRequest:
    selected = validate_reuse(receipt)["selected"]
    authority = authority or SessionConfigurationRequest()
    binding = native_binding or {}
    if binding.get("decision_id") != receipt["decision_id"]:
        binding = {}
    # Model-related legacy aliases were already consumed during resolution.
    # Passing them again could silently override explicit Automatic or a rule.
    return SessionConfigurationRequest.from_values(
        model_id=selected.get("model") or binding.get("model_id"),
        reasoning=selected.get("reasoning") or binding.get("reasoning"),
        model_provider=selected.get("native_model_provider")
        if selected.get("connection_revision")
        else selected.get("model_provider"),
        mode_id=authority.mode_id,
        config=selected.get("options", {}),
    )
