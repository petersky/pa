"""Shared limits for card material across intake, storage and workers."""

ISSUE_DETAIL_CHARACTERS = 16_000
CONVERSATION_ISSUES = 100
START_SELECTED_ISSUES = 20
CARD_ATTACHMENTS = 64
ATTACHMENT_BYTES = 25 * 1024 * 1024
CARD_ATTACHMENT_BYTES = 100 * 1024 * 1024


def material_limits() -> dict[str, int | str]:
    return {
        "issue_detail_characters": ISSUE_DETAIL_CHARACTERS,
        "conversation_issues": CONVERSATION_ISSUES,
        "start_selected_issues": START_SELECTED_ISSUES,
        "card_attachments": CARD_ATTACHMENTS,
        "attachment_bytes": ATTACHMENT_BYTES,
        "card_attachment_bytes": CARD_ATTACHMENT_BYTES,
        "overflow": "Keep discussion concise; create a card with the full description and link its card_id. Use durable card attachments for larger files.",
    }
