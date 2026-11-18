"""Responses shared by durable attachment mutations and their API receipts."""


def removal_result(card, attachment_id: str, retained: bool, prior_evidence_retained: bool = False) -> dict:
    return {**card.model_dump(mode="json"), "completion_status": card.completion_status,
            "attachment_removal": {
                "attachment_id": attachment_id,
                "retained_for_evidence": retained,
                "prior_evidence_retained": prior_evidence_retained,
                "message": ("Removed from current materials. The original remains accessible from completion evidence."
                            if retained else
                            "Removed current materials. Current content is no longer accessible; earlier revisions retained by completion records remain accessible from their evidence links."
                            if prior_evidence_retained else
                            "Removed from current materials. Links to this attachment are no longer available."),
            }}
