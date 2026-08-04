from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pa.intake.models import (
    ArtifactState,
    Channel,
    IntakeArtifact,
    IntakeDirection,
    IntakeEnvelope,
    IntakeKind,
    IntakeVisibility,
    Modality,
    ReplyCapabilities,
    SecurityAssessment,
    SenderIdentity,
    ThreadContext,
)


class AdapterError(ValueError):
    pass


def _stable_id(*parts: object) -> str:
    return str(
        uuid5(NAMESPACE_URL, "pa:intake:" + ":".join(str(part) for part in parts))
    )


def _timestamp(value: Any, *, fallback: datetime | None = None) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return fallback or datetime.now(UTC)


def _modality(media_type: str | None, *, voice: bool = False) -> Modality:
    normalized = (media_type or "").lower()
    if voice:
        return Modality.VOICE
    if normalized.startswith("image/"):
        return Modality.IMAGE
    if normalized.startswith("audio/"):
        return Modality.AUDIO
    if normalized.startswith("video/"):
        return Modality.VIDEO
    return Modality.FILE


class TelegramAdapter:
    channel = Channel.TELEGRAM

    def normalize(
        self, update: dict[str, Any], *, realm_id: str = "default"
    ) -> IntakeEnvelope:
        update_id = str(update.get("update_id") or "")
        if reaction := update.get("message_reaction"):
            return self._reaction(reaction, update_id=update_id, realm_id=realm_id)
        message_key = next(
            (
                key
                for key in (
                    "message",
                    "edited_message",
                    "channel_post",
                    "edited_channel_post",
                    "business_message",
                    "edited_business_message",
                    "guest_message",
                )
                if isinstance(update.get(key), dict)
            ),
            None,
        )
        if not message_key:
            raise AdapterError("unsupported Telegram update")
        message = dict(update[message_key])
        chat = message.get("chat") or {}
        sender = message.get("from") or message.get("sender_chat") or {}
        conversation_id = str(chat.get("id") or "")
        message_id = str(message.get("message_id") or "")
        if not conversation_id or not message_id:
            raise AdapterError("Telegram message lacks chat or message identity")
        text = str(message.get("text") or message.get("caption") or "").strip() or None
        artifacts = self._artifacts(message)
        edited = message_key.startswith("edited_")
        kind = IntakeKind.MESSAGE_EDIT if edited else IntakeKind.MESSAGE
        if text and text.startswith("/"):
            kind = IntakeKind.COMMAND
        chat_type = str(chat.get("type") or "private")
        visibility = (
            IntakeVisibility.PRIVATE
            if chat_type == "private"
            else IntakeVisibility.THREAD
            if message.get("message_thread_id")
            else IntakeVisibility.CHANNEL
            if chat_type == "channel"
            else IntakeVisibility.GROUP
        )
        envelope_id = _stable_id(Channel.TELEGRAM, conversation_id, message_id)
        return IntakeEnvelope(
            id=envelope_id,
            direction=IntakeDirection.INBOUND,
            channel=Channel.TELEGRAM,
            kind=kind,
            channel_message_id=message_id,
            correlation_id=envelope_id,
            sender=SenderIdentity(
                channel_user_id=str(sender.get("id") or conversation_id),
                username=sender.get("username"),
                display_name=" ".join(
                    part
                    for part in (sender.get("first_name"), sender.get("last_name"))
                    if part
                )
                or sender.get("title"),
                is_bot=bool(sender.get("is_bot")),
            ),
            thread=ThreadContext(
                conversation_id=conversation_id,
                thread_id=(
                    str(message["message_thread_id"])
                    if message.get("message_thread_id") is not None
                    else None
                ),
                reply_to_message_id=(
                    str((message.get("reply_to_message") or {}).get("message_id"))
                    if (message.get("reply_to_message") or {}).get("message_id")
                    is not None
                    else None
                ),
            ),
            realm_id=realm_id,
            visibility=visibility,
            occurred_at=_timestamp(message.get("edit_date") or message.get("date")),
            text=text,
            artifacts=artifacts,
            reply_capabilities=ReplyCapabilities(
                can_reply=True,
                can_edit=True,
                can_react=True,
                can_report_progress=True,
                maximum_text_length=4096,
            ),
            security=SecurityAssessment(authenticated=True),
            metadata={
                "provider_update_id": update_id,
                "provider_event": message_key,
                "conversation_type": chat_type,
                "business_connection_id": message.get("business_connection_id"),
            },
        )

    def _reaction(
        self, reaction: dict[str, Any], *, update_id: str, realm_id: str
    ) -> IntakeEnvelope:
        chat = reaction.get("chat") or {}
        sender = reaction.get("user") or reaction.get("actor_chat") or {}
        conversation_id = str(chat.get("id") or "")
        message_id = str(reaction.get("message_id") or "")
        values = reaction.get("new_reaction") or reaction.get("old_reaction") or []
        names = [
            str(
                item.get("emoji")
                or item.get("custom_emoji_id")
                or item.get("type")
                or ""
            )
            for item in values
            if isinstance(item, dict)
        ]
        value = " ".join(item for item in names if item) or "removed"
        event_identity = update_id or f"{message_id}:{sender.get('id')}:{value}"
        envelope_id = _stable_id(
            Channel.TELEGRAM, conversation_id, "reaction", event_identity
        )
        return IntakeEnvelope(
            id=envelope_id,
            channel=Channel.TELEGRAM,
            kind=IntakeKind.REACTION,
            channel_message_id=message_id,
            correlation_id=_stable_id(Channel.TELEGRAM, conversation_id, message_id),
            sender=SenderIdentity(
                channel_user_id=str(sender.get("id") or conversation_id),
                username=sender.get("username"),
                display_name=sender.get("first_name") or sender.get("title"),
            ),
            thread=ThreadContext(conversation_id=conversation_id),
            realm_id=realm_id,
            visibility=(
                IntakeVisibility.PRIVATE
                if chat.get("type") == "private"
                else IntakeVisibility.GROUP
            ),
            occurred_at=_timestamp(reaction.get("date")),
            modalities=[Modality.REACTION],
            reaction=value,
            reply_capabilities=ReplyCapabilities(can_react=True),
            security=SecurityAssessment(authenticated=True),
            metadata={
                "provider_update_id": update_id,
                "provider_event": "message_reaction",
            },
        )

    @staticmethod
    def _artifacts(message: dict[str, Any]) -> list[IntakeArtifact]:
        artifacts: list[IntakeArtifact] = []
        if photos := message.get("photo"):
            photo = max(photos, key=lambda item: int(item.get("file_size") or 0))
            artifacts.append(
                IntakeArtifact(
                    modality=Modality.IMAGE,
                    provider_file_id=photo.get("file_id"),
                    filename=f"telegram-{photo.get('file_unique_id') or 'photo'}.jpg",
                    media_type="image/jpeg",
                    size=photo.get("file_size"),
                    width=photo.get("width"),
                    height=photo.get("height"),
                )
            )
        for key, modality in (
            ("voice", Modality.VOICE),
            ("audio", Modality.AUDIO),
            ("video", Modality.VIDEO),
            ("video_note", Modality.VIDEO),
            ("document", Modality.FILE),
            ("animation", Modality.VIDEO),
        ):
            item = message.get(key)
            if not isinstance(item, dict):
                continue
            artifacts.append(
                IntakeArtifact(
                    modality=modality,
                    provider_file_id=item.get("file_id"),
                    filename=item.get("file_name")
                    or f"telegram-{key}-{item.get('file_unique_id') or 'file'}",
                    media_type=item.get("mime_type"),
                    size=item.get("file_size"),
                    duration_seconds=item.get("duration"),
                    width=item.get("width"),
                    height=item.get("height"),
                )
            )
        return artifacts


class DiscordAdapter:
    channel = Channel.DISCORD
    MESSAGE_EVENTS = {"MESSAGE_CREATE", "MESSAGE_UPDATE"}
    REACTION_EVENTS = {"MESSAGE_REACTION_ADD", "MESSAGE_REACTION_REMOVE"}

    def normalize_gateway(
        self,
        event_name: str,
        payload: dict[str, Any],
        *,
        sequence: int | None = None,
        realm_id: str = "default",
    ) -> IntakeEnvelope:
        if event_name in self.MESSAGE_EVENTS:
            return self._message(event_name, payload, realm_id=realm_id)
        if event_name in self.REACTION_EVENTS:
            return self._reaction(
                event_name, payload, sequence=sequence, realm_id=realm_id
            )
        raise AdapterError(f"unsupported Discord gateway event: {event_name}")

    def _message(
        self, event_name: str, message: dict[str, Any], *, realm_id: str
    ) -> IntakeEnvelope:
        channel_id = str(message.get("channel_id") or "")
        message_id = str(message.get("id") or "")
        if not channel_id or not message_id:
            raise AdapterError("Discord message lacks channel or message identity")
        author = message.get("author") or {}
        member = message.get("member") or {}
        parent_id = message.get("_thread_parent_id")
        text = str(message.get("content") or "").strip() or None
        artifacts = []
        voice_message = bool(int(message.get("flags") or 0) & (1 << 13))
        for item in message.get("attachments") or []:
            media_type = item.get("content_type")
            artifacts.append(
                IntakeArtifact(
                    modality=_modality(media_type, voice=voice_message),
                    provider_file_id=str(item.get("id") or "") or None,
                    source_url=item.get("url"),
                    filename=item.get("filename"),
                    media_type=media_type,
                    size=item.get("size"),
                    duration_seconds=item.get("duration_secs"),
                    width=item.get("width"),
                    height=item.get("height"),
                    state=ArtifactState.REFERENCED,
                )
            )
        kind = (
            IntakeKind.MESSAGE_EDIT
            if event_name == "MESSAGE_UPDATE"
            else IntakeKind.COMMAND
            if text and text.startswith("/")
            else IntakeKind.MESSAGE
        )
        envelope_id = _stable_id(Channel.DISCORD, channel_id, message_id)
        reply = message.get("message_reference") or {}
        return IntakeEnvelope(
            id=envelope_id,
            channel=Channel.DISCORD,
            kind=kind,
            channel_message_id=message_id,
            correlation_id=envelope_id,
            sender=SenderIdentity(
                channel_user_id=str(author.get("id") or "unknown"),
                username=author.get("username"),
                display_name=member.get("nick") or author.get("global_name"),
                is_bot=bool(author.get("bot")) or bool(message.get("webhook_id")),
            ),
            thread=ThreadContext(
                conversation_id=channel_id,
                thread_id=channel_id if parent_id else None,
                parent_conversation_id=str(parent_id) if parent_id else None,
                reply_to_message_id=(
                    str(reply["message_id"]) if reply.get("message_id") else None
                ),
            ),
            realm_id=realm_id,
            visibility=(
                IntakeVisibility.PRIVATE
                if not message.get("guild_id")
                else IntakeVisibility.THREAD
                if parent_id
                else IntakeVisibility.CHANNEL
            ),
            occurred_at=_timestamp(
                message.get("edited_timestamp") or message.get("timestamp")
            ),
            locale=message.get("locale"),
            text=text,
            artifacts=artifacts,
            reply_capabilities=ReplyCapabilities(
                can_reply=True,
                can_edit=True,
                can_react=True,
                can_report_progress=True,
                maximum_text_length=2000,
            ),
            security=SecurityAssessment(authenticated=True),
            metadata={
                "provider_event": event_name,
                "guild_id": message.get("guild_id"),
                "nonce": message.get("nonce"),
            },
        )

    def _reaction(
        self,
        event_name: str,
        payload: dict[str, Any],
        *,
        sequence: int | None,
        realm_id: str,
    ) -> IntakeEnvelope:
        channel_id = str(payload.get("channel_id") or "")
        message_id = str(payload.get("message_id") or "")
        user_id = str(payload.get("user_id") or "")
        emoji = payload.get("emoji") or {}
        value = str(emoji.get("name") or emoji.get("id") or "reaction")
        action = "added" if event_name.endswith("ADD") else "removed"
        event_identity = (
            sequence
            if sequence is not None
            else f"{message_id}:{user_id}:{value}:{action}"
        )
        envelope_id = _stable_id(
            Channel.DISCORD, channel_id, "reaction", event_identity
        )
        return IntakeEnvelope(
            id=envelope_id,
            channel=Channel.DISCORD,
            kind=IntakeKind.REACTION,
            channel_message_id=message_id,
            correlation_id=_stable_id(Channel.DISCORD, channel_id, message_id),
            sender=SenderIdentity(channel_user_id=user_id or "unknown"),
            thread=ThreadContext(
                conversation_id=channel_id,
                thread_id=channel_id if payload.get("_thread_parent_id") else None,
                parent_conversation_id=payload.get("_thread_parent_id"),
            ),
            realm_id=realm_id,
            visibility=(
                IntakeVisibility.PRIVATE
                if not payload.get("guild_id")
                else IntakeVisibility.THREAD
                if payload.get("_thread_parent_id")
                else IntakeVisibility.CHANNEL
            ),
            modalities=[Modality.REACTION],
            reaction=f"{action}:{value}",
            reply_capabilities=ReplyCapabilities(can_react=True),
            security=SecurityAssessment(authenticated=True),
            metadata={
                "provider_event": event_name,
                "guild_id": payload.get("guild_id"),
            },
        )
