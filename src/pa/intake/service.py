from __future__ import annotations

import base64
import hashlib
import io
import json
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pa.attachments import AttachmentStore
from pa.domain.models import CardEvent, EventType
from pa.intake.models import (
    ArtifactState,
    Channel,
    CorrelatedResponseCreate,
    DeliveryReceipt,
    DerivedRepresentation,
    IdentityBinding,
    IdentityConfidence,
    IntakeArtifact,
    IntakeDirection,
    IntakeDisposition,
    IntakeEnvelope,
    IntakeKind,
    IntakeMutationContext,
    IntakeVisibility,
    LinkChallengeResult,
    Modality,
    ReceiptCreate,
    ReceiptState,
    RedactionCreate,
    ReplyCapabilities,
    SecurityAssessment,
    SenderIdentity,
    ThreadContext,
)
from pa.intake.projection import (
    consume_link_challenge,
    find_intake_event_by_idempotency,
    get_envelope_payload,
    get_identity_payload,
    list_envelope_payloads,
    list_identity_payloads,
    put_link_challenge,
    referenced_blob_digests,
)
from pa.intake.security import (
    SlidingWindowLimiter,
    detect_prompt_injection,
    inspect_artifact,
    redact_channel_text,
)
from pa.intake.transports import ChannelTransport, ChannelTransportError


class IntakeConflict(ValueError):
    pass


class IntakeRejected(ValueError):
    def __init__(self, code: str, message: str, *, status_code: int = 403) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class IntakeService:
    def __init__(
        self,
        store,
        settings,
        *,
        attachments: AttachmentStore | None = None,
        transport: ChannelTransport | None = None,
    ) -> None:
        self.store = store
        self.settings = settings
        self.instance_id = settings.instance_id
        self.attachments = attachments or AttachmentStore(settings.data_dir)
        self.transport = transport or ChannelTransport(
            telegram_bot_token=getattr(settings, "telegram_bot_token", ""),
            discord_bot_token=getattr(settings, "discord_bot_token", ""),
        )
        self.limiter = SlidingWindowLimiter()

    def close(self) -> None:
        self.transport.close()

    def get(self, envelope_id: str) -> IntakeEnvelope | None:
        payload = get_envelope_payload(self.store, envelope_id)
        return IntakeEnvelope.model_validate(payload) if payload else None

    def list(
        self,
        *,
        realm_id: str | None = None,
        channel: Channel | None = None,
        correlation_id: str | None = None,
        limit: int = 100,
    ) -> list[IntakeEnvelope]:
        return [
            IntakeEnvelope.model_validate(payload)
            for payload in list_envelope_payloads(
                self.store,
                realm_id=realm_id,
                channel=channel.value if channel else None,
                correlation_id=correlation_id,
                limit=limit,
            )
        ]

    def identity(
        self, realm_id: str, channel: Channel, channel_user_id: str
    ) -> IdentityBinding | None:
        payload = get_identity_payload(
            self.store, realm_id, channel.value, channel_user_id
        )
        if not payload:
            return None
        binding = IdentityBinding.model_validate(payload)
        return None if binding.revoked_at else binding

    def ingest(
        self,
        envelope: IntakeEnvelope,
        context: IntakeMutationContext,
        *,
        raw_payload: bytes | None = None,
        fetch_artifacts: bool = True,
    ) -> IntakeEnvelope:
        duplicate = find_intake_event_by_idempotency(
            self.store, envelope.realm_id, context.idempotency_key
        )
        if duplicate:
            existing = self.get(str(duplicate["entity_id"]))
            if existing:
                return existing
        self._apply_route(envelope)
        if envelope.retention.policy == "standard":
            envelope.retention.raw_expires_at = envelope.received_at + timedelta(
                hours=float(getattr(self.settings, "intake_raw_retention_hours", 168))
            )
            envelope.retention.canonical_expires_at = envelope.received_at + timedelta(
                hours=float(
                    getattr(self.settings, "intake_canonical_retention_hours", 2160)
                )
            )
        self._authorize(envelope)
        envelope.text = redact_channel_text(envelope.text)
        if detect_prompt_injection(envelope.text):
            envelope.security.prompt_injection_detected = True
            envelope.security.reasons.append(
                "possible prompt injection; content remains untrusted"
            )
        if raw_payload is not None:
            max_bytes = int(
                getattr(self.settings, "intake_max_event_bytes", 2 * 1024 * 1024)
            )
            if len(raw_payload) > max_bytes:
                raise IntakeRejected(
                    "payload_too_large",
                    "channel payload exceeds the configured limit",
                    status_code=413,
                )
            digest, size = self.attachments.ingest(
                io.BytesIO(raw_payload), expected_size=len(raw_payload)
            )
            envelope.raw_payload_sha256 = digest
            envelope.raw_blob_ref = f"sha256:{digest}"
            envelope.raw_storage_instance_id = self.instance_id
            if size != len(raw_payload):
                raise IntakeRejected(
                    "payload_size_mismatch",
                    "raw payload storage failed",
                    status_code=400,
                )
        if fetch_artifacts:
            self._fetch_artifacts(envelope)
        current = self.get(envelope.id)
        if current:
            envelope.version = current.version + 1
            envelope.created_at = current.created_at
            envelope.correlation_id = current.correlation_id
            if envelope.kind == IntakeKind.MESSAGE_EDIT and not envelope.artifacts:
                envelope.artifacts = current.artifacts
            envelope.receipts = current.receipts
        envelope.updated_at = datetime.now(UTC)
        self._commit(envelope, "intake.received", context)
        return envelope

    def ingest_web_prompt(
        self,
        *,
        principal_id: str,
        session_id: str,
        message: str,
        images: list[Any],
        realm_id: str,
        project_id: str | None,
        goal_ids: list[str] | None,
        channel_message_id: str,
        context: IntakeMutationContext,
    ) -> IntakeEnvelope:
        artifacts: list[IntakeArtifact] = []
        public_images = []
        for image in images:
            content = base64.b64decode(image.data)
            clean, reason = inspect_artifact(
                content, filename=image.name, media_type=image.mime_type
            )
            digest, size = self.attachments.ingest(
                io.BytesIO(content), expected_size=len(content)
            )
            artifacts.append(
                IntakeArtifact(
                    modality=Modality.IMAGE,
                    filename=image.name,
                    media_type=image.mime_type,
                    size=size,
                    sha256=digest,
                    blob_ref=f"sha256:{digest}",
                    storage_instance_id=self.instance_id,
                    state=ArtifactState.STORED if clean else ArtifactState.QUARANTINED,
                    quarantine_reason=reason,
                )
            )
            public_images.append(
                {"name": image.name, "mime_type": image.mime_type, "sha256": digest}
            )
        envelope_id = str(
            uuid5(NAMESPACE_URL, f"pa:intake:web:{session_id}:{channel_message_id}")
        )
        envelope = IntakeEnvelope(
            id=envelope_id,
            channel=Channel.WEB,
            channel_message_id=channel_message_id,
            correlation_id=envelope_id,
            sender=SenderIdentity(
                channel_user_id=principal_id,
                principal_id=principal_id,
                confidence=IdentityConfidence.LINKED,
            ),
            thread=ThreadContext(conversation_id=session_id),
            realm_id=realm_id,
            project_id=project_id,
            goal_ids=goal_ids or [],
            visibility=IntakeVisibility.PRIVATE,
            text=message or None,
            artifacts=artifacts,
            reply_capabilities=ReplyCapabilities(
                can_reply=True,
                can_report_progress=True,
                maximum_text_length=32_000,
            ),
            security=SecurityAssessment(
                authenticated=True,
                allowlisted=True,
                identity_linked=True,
                disposition=(
                    IntakeDisposition.QUARANTINED
                    if any(
                        item.state == ArtifactState.QUARANTINED for item in artifacts
                    )
                    else IntakeDisposition.ACCEPTED
                ),
                untrusted_content=True,
            ),
            metadata={"session_id": session_id, "surface": "agent_chat"},
        )
        raw = json.dumps(
            {"message": message, "images": public_images},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return self.ingest(envelope, context, raw_payload=raw, fetch_artifacts=False)

    def begin_link(
        self,
        *,
        principal_id: str,
        channel: Channel,
        realm_id: str,
        expires_in_seconds: int = 600,
    ) -> LinkChallengeResult:
        code = secrets.token_urlsafe(9).replace("-", "").replace("_", "")[:10].upper()
        expires_at = datetime.now(UTC) + timedelta(seconds=expires_in_seconds)
        put_link_challenge(
            self.store,
            code_hash=hashlib.sha256(code.encode()).hexdigest(),
            channel=channel.value,
            realm_id=realm_id,
            principal_id=principal_id,
            expires_at=expires_at,
        )
        return LinkChallengeResult(channel=channel, code=code, expires_at=expires_at)

    def verify_link(
        self,
        *,
        channel: Channel,
        code: str,
        channel_user_id: str,
        conversation_id: str,
        context: IntakeMutationContext,
    ) -> IdentityBinding:
        challenge = consume_link_challenge(
            self.store,
            code_hash=hashlib.sha256(code.strip().upper().encode()).hexdigest(),
            channel=channel.value,
            now=datetime.now(UTC),
        )
        if not challenge:
            raise IntakeRejected(
                "invalid_link_code",
                "link code is invalid, expired, or already used",
                status_code=400,
            )
        existing = self.identity(str(challenge["realm_id"]), channel, channel_user_id)
        if existing and existing.principal_id != challenge["principal_id"]:
            raise IntakeConflict(
                "channel identity is already linked to another principal"
            )
        binding = existing or IdentityBinding(
            id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"pa:identity:{challenge['realm_id']}:{channel.value}:{channel_user_id}",
                )
            ),
            realm_id=str(challenge["realm_id"]),
            channel=channel,
            channel_user_id=channel_user_id,
            principal_id=str(challenge["principal_id"]),
        )
        if conversation_id not in binding.conversation_ids:
            binding.conversation_ids.append(conversation_id)
        if existing:
            binding.version += 1
        self._commit_identity(binding, "identity.linked", context)
        return binding

    def add_representation(
        self,
        envelope_id: str,
        representation: DerivedRepresentation,
        context: IntakeMutationContext,
    ) -> IntakeEnvelope:
        def change(envelope: IntakeEnvelope) -> None:
            artifact = next(
                (
                    item
                    for item in envelope.artifacts
                    if item.id == representation.derived_from_artifact_id
                ),
                None,
            )
            if not artifact:
                raise IntakeConflict(
                    "derived representation references an unknown artifact"
                )
            if not any(
                item.id == representation.id for item in artifact.representations
            ):
                artifact.representations.append(representation)

        return self._mutate(
            envelope_id, context, "intake.representation_recorded", change
        )

    def record_receipt(
        self,
        envelope_id: str,
        receipt: ReceiptCreate,
        context: IntakeMutationContext,
    ) -> IntakeEnvelope:
        def change(envelope: IntakeEnvelope) -> None:
            envelope.receipts.append(DeliveryReceipt(**receipt.model_dump()))

        return self._mutate(envelope_id, context, "intake.delivery_receipt", change)

    def redact(
        self,
        envelope_id: str,
        request: RedactionCreate,
        context: IntakeMutationContext,
    ) -> IntakeEnvelope:
        target = self.get(envelope_id)
        if not target:
            raise KeyError(envelope_id)
        prior_digests = {
            digest
            for digest in [
                target.raw_payload_sha256,
                *(artifact.sha256 for artifact in target.artifacts),
            ]
            if digest
        }

        def change(envelope: IntakeEnvelope) -> None:
            if request.redact_text:
                envelope.text = "[REDACTED]"
            envelope.raw_payload_sha256 = None
            envelope.raw_blob_ref = None
            envelope.raw_storage_instance_id = None
            if request.redact_artifacts:
                for artifact in envelope.artifacts:
                    artifact.source_url = None
                    artifact.filename = None
                    artifact.provider_file_id = None
                    artifact.sha256 = None
                    artifact.blob_ref = None
                    artifact.storage_instance_id = None
                    artifact.state = ArtifactState.REDACTED
                    artifact.quarantine_reason = request.reason
            if request.redact_identity:
                envelope.sender.username = None
                envelope.sender.display_name = None
                envelope.sender.channel_user_id = "redacted"
            envelope.security.disposition = IntakeDisposition.REDACTED
            envelope.retention.redacted_at = datetime.now(UTC)
            envelope.retention.redaction_reason = request.reason

        result = self._mutate(envelope_id, context, "intake.redacted", change)
        self._remove_unreferenced_blobs(prior_digests)
        return result

    def send_response(
        self,
        source_id: str,
        response: CorrelatedResponseCreate,
        context: IntakeMutationContext,
    ) -> IntakeEnvelope:
        source = self.get(source_id)
        if not source:
            raise KeyError(source_id)
        if source.security.disposition in {
            IntakeDisposition.REJECTED,
            IntakeDisposition.REDACTED,
        }:
            raise IntakeConflict(
                "responses are not allowed for rejected or redacted intake"
            )
        channel = response.target_channel or source.channel
        conversation_id = (
            response.target_conversation_id or source.thread.conversation_id
        )
        thread_id = response.target_thread_id or (
            source.thread.thread_id if channel == source.channel else None
        )
        reply_to = response.reply_to_message_id or (
            source.channel_message_id if channel == source.channel else None
        )
        self._authorize_response_target(source, channel, conversation_id)
        envelope_id = str(
            uuid5(NAMESPACE_URL, f"pa:intake:response:{context.idempotency_key}")
        )
        pending = DeliveryReceipt(state=ReceiptState.PENDING)
        outbound = IntakeEnvelope(
            id=envelope_id,
            direction=IntakeDirection.OUTBOUND,
            channel=channel,
            channel_message_id=envelope_id,
            correlation_id=source.correlation_id,
            in_reply_to_envelope_id=source.id,
            sender=SenderIdentity(
                channel_user_id="pa",
                display_name="PA",
                principal_id=context.actor_principal,
                confidence=IdentityConfidence.LINKED,
                is_bot=True,
            ),
            thread=ThreadContext(
                conversation_id=conversation_id,
                thread_id=thread_id,
                reply_to_message_id=reply_to,
            ),
            realm_id=source.realm_id,
            project_id=source.project_id,
            goal_ids=source.goal_ids,
            visibility=source.visibility,
            text=response.text,
            reply_capabilities=source.reply_capabilities,
            security=SecurityAssessment(
                authenticated=True,
                allowlisted=True,
                identity_linked=True,
                untrusted_content=False,
            ),
            retention=source.retention.model_copy(deep=True),
            receipts=[pending],
            metadata={"source_channel": source.channel.value},
        )
        duplicate = find_intake_event_by_idempotency(
            self.store, source.realm_id, context.idempotency_key
        )
        if duplicate:
            existing = self.get(str(duplicate["entity_id"]))
            if existing:
                return existing
        self._commit(outbound, "intake.response_queued", context)
        try:
            delivery = self._deliver(outbound)
        except ChannelTransportError as exc:
            failed = ReceiptCreate(state=ReceiptState.FAILED, detail=str(exc))
            failed_context = context.model_copy(
                update={
                    "idempotency_key": context.idempotency_key + ":failed",
                    "expected_version": outbound.version,
                }
            )
            self.record_receipt(outbound.id, failed, failed_context)
            raise
        sent = ReceiptCreate(
            state=(
                ReceiptState.DELIVERED if channel == Channel.WEB else ReceiptState.SENT
            ),
            provider_message_id=delivery.get("provider_message_id"),
            provider_delivery_id=delivery.get("provider_delivery_id"),
        )
        sent_context = context.model_copy(
            update={
                "idempotency_key": context.idempotency_key + ":sent",
                "expected_version": outbound.version,
            }
        )
        return self.record_receipt(outbound.id, sent, sent_context)

    def retention_sweep(
        self, *, now: datetime | None = None, limit: int = 500
    ) -> dict[str, int]:
        current = now or datetime.now(UTC)
        candidates = self.list(limit=min(max(limit, 1), 5000))
        prior_digests = referenced_blob_digests(self.store)
        raw_redacted = canonical_redacted = 0
        for envelope in candidates:
            canonical_expiry = envelope.retention.canonical_expires_at
            raw_expiry = envelope.retention.raw_expires_at
            if envelope.retention.policy == "legal_hold":
                continue
            if canonical_expiry and canonical_expiry <= current:
                self.redact(
                    envelope.id,
                    RedactionCreate(
                        reason="canonical retention expired", redact_identity=True
                    ),
                    IntakeMutationContext(
                        actor_principal="system:retention",
                        authority_instance_id=self.instance_id,
                        idempotency_key=f"retention:canonical:{envelope.id}:{canonical_expiry.isoformat()}",
                        expected_version=envelope.version,
                    ),
                )
                canonical_redacted += 1
                continue
            if (
                raw_expiry
                and raw_expiry <= current
                and (
                    envelope.raw_blob_ref
                    or any(item.blob_ref for item in envelope.artifacts)
                )
            ):

                def expire_raw(item: IntakeEnvelope) -> None:
                    item.raw_payload_sha256 = None
                    item.raw_blob_ref = None
                    item.raw_storage_instance_id = None
                    for artifact in item.artifacts:
                        artifact.source_url = None
                        artifact.provider_file_id = None
                        artifact.sha256 = None
                        artifact.blob_ref = None
                        artifact.storage_instance_id = None
                        artifact.state = ArtifactState.REDACTED
                        artifact.quarantine_reason = "raw retention expired"

                self._mutate(
                    envelope.id,
                    IntakeMutationContext(
                        actor_principal="system:retention",
                        authority_instance_id=self.instance_id,
                        idempotency_key=f"retention:raw:{envelope.id}:{raw_expiry.isoformat()}",
                        expected_version=envelope.version,
                    ),
                    "intake.raw_expired",
                    expire_raw,
                )
                raw_redacted += 1
        removed = self._remove_unreferenced_blobs(prior_digests)
        return {
            "raw_redacted": raw_redacted,
            "canonical_redacted": canonical_redacted,
            "removed_blobs": removed,
        }

    def _remove_unreferenced_blobs(self, candidates: set[str]) -> int:
        remaining = referenced_blob_digests(self.store)
        removed = 0
        for digest in candidates - remaining:
            path = self.attachments.blob_path(digest)
            if path.is_file() and path.stat().st_nlink == 1:
                path.unlink()
                removed += 1
        return removed

    def _authorize(self, envelope: IntakeEnvelope) -> None:
        if not envelope.security.authenticated:
            raise IntakeRejected(
                "unauthenticated_channel",
                "channel authentication failed",
                status_code=401,
            )
        if envelope.channel == Channel.WEB:
            envelope.security.allowlisted = True
            envelope.security.identity_linked = bool(envelope.sender.principal_id)
            return
        if envelope.sender.is_bot:
            raise IntakeRejected(
                "bot_loop_prevented", "messages generated by bots are not admitted"
            )
        binding = self.identity(
            envelope.realm_id, envelope.channel, envelope.sender.channel_user_id
        )
        if binding:
            envelope.sender.principal_id = binding.principal_id
            envelope.sender.confidence = IdentityConfidence.LINKED
            envelope.security.identity_linked = True
        allowed_users = {
            str(item)
            for item in getattr(
                self.settings, f"{envelope.channel.value}_allowed_user_ids", []
            )
        }
        allowed_conversations = {
            str(item)
            for item in getattr(
                self.settings, f"{envelope.channel.value}_allowed_conversation_ids", []
            )
        }
        envelope.security.allowlisted = (
            envelope.sender.channel_user_id in allowed_users
            or envelope.thread.conversation_id in allowed_conversations
        )
        if not (binding or envelope.security.allowlisted):
            raise IntakeRejected(
                "identity_not_authorized",
                "channel identity must be linked or explicitly allowlisted",
            )
        identity_limit = int(getattr(self.settings, "intake_identity_rate_limit", 30))
        conversation_limit = int(
            getattr(self.settings, "intake_conversation_rate_limit", 120)
        )
        if not self.limiter.allow(
            f"identity:{envelope.channel}:{envelope.sender.channel_user_id}",
            limit=identity_limit,
        ) or not self.limiter.allow(
            f"conversation:{envelope.channel}:{envelope.thread.conversation_id}",
            limit=conversation_limit,
        ):
            raise IntakeRejected(
                "rate_limited", "channel intake rate limit exceeded", status_code=429
            )

    def _apply_route(self, envelope: IntakeEnvelope) -> None:
        routes = getattr(self.settings, "intake_channel_routes", {}) or {}
        route = routes.get(
            f"{envelope.channel.value}:{envelope.thread.conversation_id}", {}
        )
        if route:
            envelope.realm_id = str(route.get("realm_id") or envelope.realm_id)
            envelope.project_id = route.get("project_id") or envelope.project_id
            envelope.goal_ids = [
                str(item) for item in route.get("goal_ids") or envelope.goal_ids
            ]
        subscribed = set(getattr(self.settings, "subscribed_realms", []) or ["default"])
        if envelope.realm_id not in subscribed:
            raise IntakeRejected(
                "realm_not_authorized", "channel route targets an unsubscribed realm"
            )

    def _fetch_artifacts(self, envelope: IntakeEnvelope) -> None:
        for artifact in envelope.artifacts:
            if artifact.size is not None and artifact.size > int(
                getattr(self.settings, "intake_max_artifact_bytes", 25 * 1024 * 1024)
            ):
                artifact.state = ArtifactState.QUARANTINED
                artifact.quarantine_reason = "artifact exceeds configured size limit"
                envelope.security.size_valid = False
                envelope.security.disposition = IntakeDisposition.QUARANTINED
                continue
            try:
                if envelope.channel == Channel.TELEGRAM and artifact.provider_file_id:
                    content = self.transport.fetch_telegram_file(
                        artifact.provider_file_id, expected_size=artifact.size
                    )
                elif envelope.channel == Channel.DISCORD and artifact.source_url:
                    content = self.transport.fetch_discord_file(
                        artifact.source_url, expected_size=artifact.size
                    )
                else:
                    raise ChannelTransportError(
                        "artifact has no retrievable provider reference"
                    )
                clean, reason = inspect_artifact(
                    content, filename=artifact.filename, media_type=artifact.media_type
                )
                digest, size = self.attachments.ingest(
                    io.BytesIO(content), expected_size=len(content)
                )
                artifact.size = size
                artifact.sha256 = digest
                artifact.blob_ref = f"sha256:{digest}"
                artifact.storage_instance_id = self.instance_id
                artifact.state = (
                    ArtifactState.STORED if clean else ArtifactState.QUARANTINED
                )
                artifact.quarantine_reason = reason
                if not clean:
                    envelope.security.malware_scan = "suspicious"
                    envelope.security.disposition = IntakeDisposition.QUARANTINED
                    envelope.security.reasons.append(reason or "artifact quarantined")
                else:
                    envelope.security.malware_scan = "clean"
            except (ChannelTransportError, OSError, ValueError) as exc:
                artifact.state = ArtifactState.QUARANTINED
                artifact.quarantine_reason = str(exc)[:1000]
                envelope.security.malware_scan = "pending"
                envelope.security.disposition = IntakeDisposition.QUARANTINED
                envelope.security.reasons.append(
                    "artifact retrieval or inspection incomplete"
                )

    def _authorize_response_target(
        self, source: IntakeEnvelope, channel: Channel, conversation_id: str
    ) -> None:
        same_audience = (
            channel == source.channel
            and conversation_id == source.thread.conversation_id
        )
        if same_audience:
            if not source.reply_capabilities.can_reply:
                raise IntakeConflict("the initiating channel does not permit replies")
            return
        if (
            source.visibility != IntakeVisibility.PRIVATE
            or not source.sender.principal_id
        ):
            raise IntakeConflict(
                "cross-channel responses require a linked private identity"
            )
        bindings = [
            IdentityBinding.model_validate(item)
            for item in list_identity_payloads(
                self.store, source.realm_id, source.sender.principal_id
            )
        ]
        if not any(
            binding.channel == channel
            and conversation_id in binding.conversation_ids
            and binding.revoked_at is None
            for binding in bindings
        ):
            raise IntakeConflict(
                "target channel audience has not been verified for this identity"
            )

    def _deliver(self, envelope: IntakeEnvelope) -> dict[str, Any]:
        if envelope.channel == Channel.WEB:
            return {
                "provider_message_id": envelope.id,
                "provider_delivery_id": envelope.id,
            }
        if envelope.channel == Channel.TELEGRAM:
            return self.transport.send_telegram(
                conversation_id=envelope.thread.conversation_id,
                thread_id=envelope.thread.thread_id,
                reply_to_message_id=envelope.thread.reply_to_message_id,
                text=envelope.text or "",
            )
        if envelope.channel == Channel.DISCORD:
            return self.transport.send_discord(
                conversation_id=envelope.thread.conversation_id,
                reply_to_message_id=envelope.thread.reply_to_message_id,
                text=envelope.text or "",
                nonce=envelope.id,
            )
        raise ChannelTransportError("unsupported delivery channel")

    def _mutate(
        self,
        envelope_id: str,
        context: IntakeMutationContext,
        action: str,
        change: Callable[[IntakeEnvelope], None],
    ) -> IntakeEnvelope:
        envelope = self.get(envelope_id)
        if not envelope:
            raise KeyError(envelope_id)
        duplicate = find_intake_event_by_idempotency(
            self.store, envelope.realm_id, context.idempotency_key
        )
        if duplicate:
            if duplicate["entity_id"] != envelope_id:
                raise IntakeConflict(
                    "idempotency key already belongs to another intake entity"
                )
            return envelope
        if (
            context.expected_version is not None
            and context.expected_version != envelope.version
        ):
            raise IntakeConflict(
                f"expected version {context.expected_version}, current version {envelope.version}"
            )
        change(envelope)
        envelope.version += 1
        envelope.updated_at = datetime.now(UTC)
        self._commit(envelope, action, context)
        return envelope

    def _commit(
        self,
        envelope: IntakeEnvelope,
        action: str,
        context: IntakeMutationContext,
    ) -> None:
        payload = envelope.model_dump(mode="json")
        payload["_event_action"] = action
        payload["_idempotency_key"] = context.idempotency_key
        self.store.commit_event(
            CardEvent(
                type=EventType.INTAKE_ENVELOPE_UPSERTED,
                realm_id=envelope.realm_id,
                project_id=envelope.project_id,
                author_principal=context.actor_principal,
                author_instance=context.authority_instance_id,
                payload=payload,
            )
        )

    def _commit_identity(
        self,
        binding: IdentityBinding,
        action: str,
        context: IntakeMutationContext,
    ) -> None:
        duplicate = find_intake_event_by_idempotency(
            self.store, binding.realm_id, context.idempotency_key
        )
        if duplicate:
            return
        payload = binding.model_dump(mode="json")
        payload["_event_action"] = action
        payload["_idempotency_key"] = context.idempotency_key
        self.store.commit_event(
            CardEvent(
                type=EventType.CHANNEL_IDENTITY_UPSERTED,
                realm_id=binding.realm_id,
                author_principal=context.actor_principal,
                author_instance=context.authority_instance_id,
                payload=payload,
            )
        )
