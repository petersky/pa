"""CLI entry point for scripts/release.sh."""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

from pa.release.notes import (
    DEFAULT_AGENT_TIMEOUT,
    generate_release_notes,
    latest_tag,
    notes_path_for_tag,
    previous_tag,
    resolve_agent_timeout,
    write_release_notes,
)
from pa.release.runner import (
    ReleaseError,
    amend_release_notes,
    commits_behind_origin_main,
    create_release,
    ensure_tag_available,
    publish_github_release,
    push_existing_release,
    resolve_version,
    wait_for_github_release,
)
from pa.release.version import read_version, tag_for_version, track_for_version


def _log(msg: str) -> None:
    print(msg, flush=True)


def _print_failure(exc: BaseException) -> None:
    """Print a user-facing error (and recovery hints) instead of a raw traceback."""
    print(f"error: {exc}", file=sys.stderr)
    hints = getattr(exc, "hints", None)
    if hints:
        print("\nRecommended options:", file=sys.stderr)
        for i, hint in enumerate(hints, start=1):
            indented = hint.replace("\n", "\n    ")
            print(f"  {i}. {indented}", file=sys.stderr)
    if isinstance(exc, ReleaseError):
        return
    # Unexpected failures: keep a short traceback for debugging.
    print("\nDetails:", file=sys.stderr)
    traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)


def _warn_if_behind_origin_main(*, require_up_to_date: bool) -> int:
    """Warn when HEAD is behind origin/main. Return 1 if the release should abort."""
    _log("==> Checking branch is up to date with origin/main...")
    try:
        behind = commits_behind_origin_main()
    except RuntimeError as exc:
        print(f"warning: could not check origin/main: {exc}", file=sys.stderr)
        return 0
    if behind <= 0:
        _log("  Branch is up to date with origin/main.")
        return 0
    noun = "commit" if behind == 1 else "commits"
    print(
        f"warning: current branch is behind origin/main by {behind} {noun}.",
        file=sys.stderr,
    )
    print(
        "  Integrate remote changes first (e.g. `git pull --rebase origin main`), then retry.",
        file=sys.stderr,
    )
    if require_up_to_date:
        print("error: aborting release while behind origin/main.", file=sys.stderr)
        return 1
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create or amend a PA release with agent-generated notes.",
    )
    parser.add_argument(
        "version",
        nargs="?",
        help="major, minor, patch, alpha, beta, rc, or explicit semver (e.g. 1.2.3)",
    )
    parser.add_argument("--channel", help="channels.json track (release, beta, alpha, dev)")
    parser.add_argument("--amend", action="store_true", help="Amend existing release notes")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Push current tag to origin and publish notes (no version bump)",
    )
    parser.add_argument("--tag", help="Tag for amend/publish (default: latest or current version)")
    parser.add_argument("--agent", dest="agent_cmd", help="Agent command")
    parser.add_argument("--agent-args", dest="agent_args", help="Agent arguments")
    parser.add_argument(
        "--agent-timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            f"Max seconds to wait for the notes agent "
            f"(default: {DEFAULT_AGENT_TIMEOUT}, or PA_RELEASE_AGENT_TIMEOUT)"
        ),
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="Skip pushing commit and tag to origin (default: push)",
    )
    parser.add_argument("--no-commit", action="store_true", help="Skip git commit")
    parser.add_argument("--no-agent", action="store_true", help="Skip agent; use prefilled template")
    parser.add_argument("--notes-file", type=Path, help="Override release notes path")
    parser.add_argument("-m", "--message", help="Commit/tag message")
    parser.add_argument("--skip-gh", action="store_true", help="Skip gh release create/edit")
    parser.add_argument(
        "--wait-ci",
        type=int,
        default=120,
        help="Max seconds to poll for CI GitHub release before publishing notes (0 to skip)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    return parser.parse_args(argv)


def _tag_version(tag: str) -> str:
    return tag[1:] if tag.startswith("v") else tag


def _wait_then_publish(tag: str, notes_path: Path, *, wait_ci: int) -> None:
    """Wait (with backoff) for CI's GitHub release, then publish notes."""
    if wait_ci > 0:
        _log(f"==> Polling for CI to create GitHub release {tag} (up to {wait_ci}s)...")
        if wait_for_github_release(tag, timeout=wait_ci):
            _log(f"  GitHub release {tag} is ready.")
        else:
            print(
                f"warning: GitHub release {tag} not found after {wait_ci}s; "
                "proceeding anyway (will create if needed).",
                file=sys.stderr,
            )
    try:
        publish_github_release(tag, notes_path, amend=False)
        _log(f"  Published release notes to GitHub for {tag}.")
    except RuntimeError as exc:
        print(f"warning: gh release publish failed: {exc}", file=sys.stderr)
        print(
            f"  Try manually: gh release edit {tag} --notes-file {notes_path}",
            file=sys.stderr,
        )


def _run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    do_push = not args.no_push
    agent_timeout = None if args.no_agent else resolve_agent_timeout(args.agent_timeout)

    if args.dry_run:
        if args.publish:
            tag = args.tag or tag_for_version(read_version())
            print(f"dry-run: publish {tag}")
        elif args.amend:
            tag = args.tag or latest_tag() or "?"
            print(f"dry-run: amend {tag}")
        elif args.version:
            new = resolve_version(args.version)
            print(f"dry-run: {args.version} -> {new} (v{new}) channel={args.channel or track_for_version(new)}")
        else:
            print("error: version required", file=sys.stderr)
            return 1
        return 0

    if args.publish:
        if _warn_if_behind_origin_main(require_up_to_date=True):
            return 1
        tag = args.tag or tag_for_version(read_version())
        if not tag.startswith("v"):
            tag = f"v{tag}"
        notes_path = args.notes_file or notes_path_for_tag(tag)
        if not notes_path.exists():
            print(f"error: release notes not found: {notes_path}", file=sys.stderr)
            return 1
        _log(f"Publishing {tag}...")
        push_existing_release(tag)
        if not args.skip_gh:
            _wait_then_publish(tag, notes_path, wait_ci=args.wait_ci)
        else:
            _log("==> Skipping GitHub release publish (--skip-gh).")
        _log(f"Done. Published {tag}")
        return 0

    if args.amend:
        if _warn_if_behind_origin_main(require_up_to_date=do_push):
            return 1
        tag = args.tag or latest_tag()
        if not tag:
            print("error: no tag to amend", file=sys.stderr)
            return 1
        if not tag.startswith("v"):
            tag = f"v{tag}"
        version = _tag_version(tag)
        channel = args.channel or track_for_version(version)
        prev = previous_tag(tag)
        _log(f"Amending release notes for {tag} (track: {channel})...")

        content = generate_release_notes(
            version=version,
            tag=tag,
            channel=channel,
            since_tag=prev,
            use_agent=not args.no_agent,
            agent_cmd=args.agent_cmd,
            agent_args=args.agent_args,
            agent_timeout=agent_timeout,
        )
        _log(f"==> Writing release notes to {args.notes_file or notes_path_for_tag(tag)}...")
        notes_path = write_release_notes(tag, content, path=args.notes_file)
        _log(f"  Wrote {notes_path}")

        amend_release_notes(
            tag,
            content,
            commit=not args.no_commit,
            push=do_push,
            message=args.message,
            notes_path=notes_path,
        )

        if not args.skip_gh and do_push:
            try:
                publish_github_release(tag, notes_path, amend=True)
                _log(f"  Updated GitHub release notes for {tag}.")
            except RuntimeError as exc:
                print(f"warning: gh release edit failed: {exc}", file=sys.stderr)
        elif args.skip_gh:
            _log("==> Skipping GitHub release publish (--skip-gh).")

        _log(f"Done. Amended {tag}")
        return 0

    if not args.version:
        print("error: version bump required (major, minor, patch, or X.Y.Z)", file=sys.stderr)
        return 1

    if _warn_if_behind_origin_main(require_up_to_date=do_push):
        return 1

    old = read_version()
    new = resolve_version(args.version)
    tag = tag_for_version(new)
    channel = args.channel or track_for_version(new)
    prev = latest_tag()

    _log(f"Releasing {old} -> {tag} (track: {channel})...")
    _log(f"==> Checking that tag {tag} is available...")
    ensure_tag_available(tag)
    _log(f"  Tag {tag} is free.")

    content = generate_release_notes(
        version=new,
        tag=tag,
        channel=channel,
        since_tag=prev,
        use_agent=not args.no_agent,
        agent_cmd=args.agent_cmd,
        agent_args=args.agent_args,
        agent_timeout=agent_timeout,
    )
    _log(f"==> Writing release notes to {args.notes_file or notes_path_for_tag(tag)}...")
    notes_path = write_release_notes(tag, content, path=args.notes_file)
    _log(f"  Wrote {notes_path}")

    result = create_release(
        args.version,
        channel=args.channel,
        commit=not args.no_commit,
        push=do_push,
        message=args.message,
        notes_content=content,
        notes_path=notes_path,
        check_tag=False,  # already verified above
    )
    _log(f"  Version bump complete: {result.old_version} -> {result.new_version} ({result.tag})")

    if not args.skip_gh and do_push:
        _wait_then_publish(tag, notes_path, wait_ci=args.wait_ci)
    elif args.skip_gh:
        _log("==> Skipping GitHub release publish (--skip-gh).")

    _log(f"\nRelease {tag} complete.")
    _log(f"  Notes: {notes_path}")
    if not do_push:
        _log("  Skipped push (--no-push). Publish later:")
        _log(f"    ./scripts/release.sh --publish --tag {tag}")
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return _run(argv)
    except ReleaseError as exc:
        _print_failure(exc)
        return 1
    except RuntimeError as exc:
        _print_failure(exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
