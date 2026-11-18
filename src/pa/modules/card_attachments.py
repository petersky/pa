"""Browser and agent attachment editing using the canonical card writer."""
import io
import json
from datetime import datetime
from html import escape
from html.parser import HTMLParser
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from pa.attachments import AttachmentError, safe_filename
from pa.domain.attachment_revisions import AttachmentRevisionConflict
from pa.domain.projection import CardVersionConflict

router = APIRouter()
TEXT_EDIT_BYTES = 1024 * 1024
TEXT_TYPES = {"text/plain", "text/markdown", "text/html", "text/css", "text/csv",
              "text/javascript", "application/json", "application/xml", "text/xml"}


class InertHTML(HTMLParser):
    """Structural HTML/CSS only. No navigation, executable or network attributes."""
    tags = set("html head body title style p div span article section header footer main aside nav h1 h2 h3 h4 h5 h6 table thead tbody tfoot tr th td caption colgroup col ul ol li dl dt dd strong em b i u s small pre code blockquote br hr details summary a img figure figcaption time sup sub".split())
    attrs = {"class", "id", "style", "title", "lang", "dir", "colspan", "rowspan", "scope", "open", "width", "height", "alt"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.output = []
        self.style = False

    def handle_starttag(self, tag, attrs):
        if tag not in self.tags:
            return
        self.style = tag == "style"
        kept = [(k, v) for k, v in attrs if k in self.attrs]
        if tag == "img":
            kept += [(k, v) for k, v in attrs if k == "src" and v and v.startswith(("data:image/png;base64,", "data:image/jpeg;base64,", "data:image/webp;base64,", "data:image/gif;base64,"))]
        self.output.append("<" + tag + "".join(' ' + k + '="' + escape(v or "", quote=True) + '"' for k, v in kept) + ">")

    def handle_endtag(self, tag):
        if tag in self.tags:
            self.output.append("</" + tag + ">")
        if tag == "style":
            self.style = False

    def handle_data(self, value):
        self.output.append(value.replace("<", "\\3c ") if self.style else escape(value))


def inert_html(source):
    parser = InertHTML()
    parser.feed(source)
    parser.close()
    return "".join(parser.output)


def items():
    from pa.modules import items as module
    return module


def valid_resource_url(value):
    url = urlsplit(value)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or any(ord(c) < 33 for c in value):
        raise ValueError("Use an http or https resource URL without credentials or whitespace")
    return value


class LinkInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)
    resource_url: str = Field(min_length=1, max_length=4096)
    _url = field_validator("resource_url")(valid_resource_url)


class MetadataInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_revision: int = Field(ge=1)
    expected_version: datetime | None = None
    name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    resource_url: str | None = Field(default=None, min_length=1, max_length=4096)

    @field_validator("resource_url")
    @classmethod
    def validate_url(cls, value):
        return valid_resource_url(value) if value is not None else value


class ContentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_revision: int = Field(ge=1)
    expected_version: datetime | None = None
    content: str = Field(max_length=TEXT_EDIT_BYTES)


def attachment_presentation(attachment):
    media = attachment.media_type.split(";", 1)[0].lower()
    preview = ("link" if attachment.kind == "resource_link" else
               "html" if media == "text/html" else
               "text" if media in TEXT_TYPES else
               "image" if media in items().SAFE_IMAGE_TYPES else
               "media" if media.startswith(("video/", "audio/")) else
               "unsupported")
    target = None
    if attachment.kind == "resource_link":
        try:
            target = valid_resource_url(attachment.resource_url or "")
        except ValueError:
            pass
    return {"target": target, "preview": preview, "editable": attachment.kind == "file" and media in TEXT_TYPES and attachment.size <= TEXT_EDIT_BYTES}


def _revision(request, card_id, attachment_id, realm, changes, expected_revision, expected_version, *, mode):
    m = items()
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    store = m.get_store()
    m._attachment_access(request, realm_id)
    key, fingerprint, replay = m._begin_operation(request, operation="card.attachment.update", realm_id=realm_id,
        payload={"card_id": card_id, "attachment_id": attachment_id, "changes": changes,
                 "expected_revision": expected_revision, "expected_version": expected_version.isoformat() if expected_version else None, "mode": mode})
    if replay is not None:
        return replay
    try:
        _card, current = m._card_attachment_or_404(request, card_id, attachment_id, realm_id)
        if mode in {"replace", "content"} and current.kind != "file":
            raise HTTPException(422, "Resource links have URL editing; only files have replace/content actions")
        if mode == "content" and not attachment_presentation(current)["editable"]:
            raise HTTPException(415, "Content editor supports UTF-8 text, HTML and JSON files up to 1 MiB. Replace this file instead.")
        if mode == "content" and current.media_type.split(";", 1)[0] == "application/json":
            try:
                json.loads(m._attachment_store(request).blob_path(changes["sha256"]).read_text("utf-8"))
            except ValueError as exc:
                raise HTTPException(422, "Invalid JSON. Correct the source or replace the file.") from exc
        if "resource_url" in changes and current.kind != "resource_link":
            raise HTTPException(422, "Only resource-link attachments have editable URLs")
        # Immutable history/evidence must survive the blob collector and replacements.
        if "sha256" in changes:
            m._ensure_attachment_available(request, current).retain_result_evidence(current.sha256)
        result = store.revise_attachment(card_id, attachment_id, realm_id=realm_id, changes=changes,
            expected_revision=expected_revision, expected_version=expected_version,
            principal_id=m.get_principal_id(request), instance_id=request.app.state.ctx.settings.instance_id,
            idempotency_key=key, request_fingerprint=fingerprint)
        if result is None:
            raise HTTPException(404, "Attachment not found")
    except (CardVersionConflict, AttachmentRevisionConflict) as exc:
        store.fail_operation(key, "stale_attachment_revision")
        raise HTTPException(409, {"code": "stale_attachment_revision", "message": "Card or attachment changed. Refresh its materials before saving again."}) from exc
    except ValueError as exc:
        store.fail_operation(key, "invalid_attachment")
        raise HTTPException(422, {"code": "invalid_attachment", "message": str(exc)}) from exc
    except Exception as exc:
        store.fail_operation(key, type(exc).__name__)
        raise
    store.complete_operation(key, result)
    return result


def _ingest(request, content):
    sha, size = items()._attachment_store(request).ingest(io.BytesIO(content))
    return {"sha256": sha, "size": size, "blob_ref": "sha256:" + sha}


@router.post("/cards/{card_id}/attachment-links", status_code=201)
def create_link(request: Request, card_id: str, body: LinkInput, realm: str | None = None):
    m = items()
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    m._require_attachment_card(request, card_id, realm_id)
    blob = _ingest(request, body.resource_url.encode("utf-8"))
    return m._publish_card_attachment(request, card_id, realm_id, blob["sha256"], blob["size"],
        "resource-link.txt", "text/uri-list", metadata={"kind": "resource_link", **body.model_dump()})


@router.patch("/cards/{card_id}/attachments/{attachment_id}")
def edit_metadata(request: Request, card_id: str, attachment_id: str, body: MetadataInput, realm: str | None = None):
    changes = body.model_dump(exclude={"expected_revision", "expected_version"}, exclude_none=True)
    if "resource_url" in changes:
        changes.update(_ingest(request, changes["resource_url"].encode("utf-8")))
    return _revision(request, card_id, attachment_id, realm, changes, body.expected_revision, body.expected_version, mode="metadata")


@router.put("/cards/{card_id}/attachments/{attachment_id}/replace")
async def replace_file(request: Request, card_id: str, attachment_id: str,
                       expected_revision: int = Query(ge=1), expected_version: datetime | None = None,
                       file: UploadFile = File(...), realm: str | None = None):
    m = items()
    runtime = request.app.state.ctx.require_service("async_runtime")
    try:
        m._require_attachment_card(request, card_id, realm)
        sha, size = await runtime.run_blocking("cards.attachments.ingest", m._attachment_store(request).ingest, file.file, timeout=120.0)
        return await runtime.run_blocking("cards.attachments.replace", _revision, request, card_id, attachment_id, realm,
            {"sha256": sha, "size": size, "blob_ref": "sha256:" + sha,
             "filename": safe_filename(file.filename or "attachment"),
             "media_type": m._attachment_media_type(file.filename or "attachment", file.content_type)},
             expected_revision, expected_version, mode="replace")
    except AttachmentError as exc:
        raise HTTPException(413 if not exc.recoverable else 409, exc.detail()) from exc
    finally:
        await file.close()


@router.get("/cards/{card_id}/attachments/{attachment_id}/content")
def get_content(request: Request, card_id: str, attachment_id: str, realm: str | None = None):
    m = items()
    card, attachment = m._card_attachment_or_404(request, card_id, attachment_id, realm)
    if not attachment_presentation(attachment)["editable"]:
        raise HTTPException(415, "No content editor for this format or size. Use Replace file.")
    try:
        content = m._ensure_attachment_available(request, attachment).blob_path(attachment.sha256).read_text("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(415, "This file is not UTF-8 text. Use Replace file.") from exc
    return {"content": content, "expected_revision": attachment.revision, "expected_version": card.updated_at.isoformat()}


@router.put("/cards/{card_id}/attachments/{attachment_id}/content")
def edit_content(request: Request, card_id: str, attachment_id: str, body: ContentInput, realm: str | None = None):
    encoded = body.content.encode("utf-8")
    if len(encoded) > TEXT_EDIT_BYTES:
        raise HTTPException(413, "Content editing is limited to 1 MiB. Use Replace file.")
    return _revision(request, card_id, attachment_id, realm, _ingest(request, encoded), body.expected_revision, body.expected_version, mode="content")


@router.get("/cards/{card_id}/attachments/{attachment_id}/preview", response_class=HTMLResponse)
def preview(request: Request, card_id: str, attachment_id: str, realm: str | None = None,
            revision: int | None = Query(default=None, ge=1)):
    m = items()
    card, attachment = m._card_attachment_or_404(request, card_id, attachment_id, realm, include_retained=True, revision=revision)
    base = f"/api/cards/{card.id}/attachments/{attachment.attachment_id}?" + urlencode({"realm": card.realm_id, "revision": attachment.revision})
    kind = attachment_presentation(attachment)["preview"]
    heading = escape(attachment.name or attachment.filename)
    download = escape(base + "&download=true", quote=True)
    back = escape("/work?" + urlencode({"card": card.id, "realm": card.realm_id}), quote=True)
    prefix = f'<!doctype html><meta charset="utf-8"><title>{heading}</title><style>body{{font:16px system-ui;margin:2rem;color:#222}}iframe{{width:100%;height:75vh;border:1px solid #aaa}}pre{{white-space:pre-wrap;overflow-wrap:anywhere}}a{{margin-right:1rem}}</style><h1>{heading}</h1><p><a href="{download}">Download original</a><a href="{back}">Card details / Replace file</a></p>'
    try:
        if kind == "link":
            # Validate again when rendering synced/untrusted metadata.
            target = escape(valid_resource_url(attachment.resource_url or ""), quote=True)
            body = f'<p>Resource link. PA does not fetch or verify the target.</p><a href="{target}" target="_blank" rel="noopener noreferrer">Open resource</a>'
        elif kind == "unsupported":
            body = '<p>No browser preview is available for this format. Download the original or use Replace file in card details.</p>'
        elif kind == "text":
            if attachment.size > TEXT_EDIT_BYTES:
                body = '<p>Text preview is limited to 1 MiB. Download the original.</p>'
            else:
                content = m._ensure_attachment_available(request, attachment).blob_path(attachment.sha256).read_text("utf-8")
                body = '<pre>' + escape(content) + '</pre>'
        elif kind == "html" and attachment.size > TEXT_EDIT_BYTES:
            body = '<p>HTML preview is limited to 1 MiB. Download the original or replace the file.</p>'
        else:
            m._ensure_attachment_available(request, attachment)
            body = f'<iframe title="Attachment preview" sandbox="" referrerpolicy="no-referrer" src="{escape(base + "&view=true", quote=True)}"></iframe>'
            if kind == "html":
                body = '<p>HTML preview: scripts, forms, navigation and network requests are disabled.</p>' + body
    except (HTTPException, UnicodeDecodeError, ValueError) as exc:
        message = "This file is not UTF-8 text. Download or replace it." if isinstance(exc, UnicodeDecodeError) else "The resource is currently unavailable. The saved attachment reference has been kept. Retry opening or download later."
        status = exc.status_code if isinstance(exc, HTTPException) else 415
        return HTMLResponse(prefix + '<p role="alert">' + message + '</p>', status_code=status, headers={"Cache-Control": "private, no-store", "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; frame-src 'self'; base-uri 'none'"})
    return HTMLResponse(prefix + body, headers={"Cache-Control": "private, no-store", "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; frame-src 'self'; base-uri 'none'", "X-Content-Type-Options": "nosniff"})
