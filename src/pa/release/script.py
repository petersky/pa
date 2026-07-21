"""CLI entry point for scripts/release.sh."""

from __future__ import annotations

import argparse
import sys
import tempfile
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
    cleanup_release_branch,
    commits_behind_origin_main,
    create_release,
    current_branch,
    ensure_release_pr,
    ensure_release_branch,
    ensure_tag_available,
    head_commit,
    merge_release_pr,
    origin_main_release_notes,
    publish_github_release,
    resolve_version,
    tag_merged_release,
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
        help="Tag the verified merged release on origin/main and publish notes (no version bump)",
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
        help="Skip pushing the release branch or tag to origin (default: push)",
    )
    parser.add_argument("--no-commit", action="store_true", help="Skip git commit")
    parser.add_argument("--no-agent", action="store_true", help="Skip agent; use prefilled template")
    parser.add_argument("--notes-file", type=Path, help="Override release notes path")
    parser.add_argument("-m", "--message", help="Commit/tag message")
    parser.add_argument("--skip-gh", action="store_true", help="Skip gh release create/edit")
    parser.add_argument(
        "--ship",
        action="store_true",
        help="Create and merge the release PR, then tag and publish without prompting",
    )
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


def _notes_path_from_merged_main(tag: str) -> tuple[Path, Path]:
    """Materialize release notes from origin/main so publish never uses stale local files."""
    temporary = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".md", prefix=f"{tag}-", delete=False
    )
    try:
        temporary.write(origin_main_release_notes(tag))
        return Path(temporary.name), Path(temporary.name)
    finally:
        temporary.close()


def _confirm_ship(tag: str, pr_url: str) -> bool:
    if not sys.stdin.isatty():
        return False
    try:
        answer = input(
            f"Wait for checks, merge {pr_url}, tag, and publish {tag}? [y/N] "
        )
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def _cleanup_finished_release_branch(tag: str, branch: str | None = None) -> None:
    """Remove the finished release PR branch after merge/publish."""
    tag = tag if tag.startswith("v") else f"v{tag}"
    if branch:
        cleanup_release_branch(branch)
        return
    try:
        current = current_branch()
    except ReleaseError:
        return
    if current in {f"release/{tag}", f"release-notes/{tag}"}:
        cleanup_release_branch(current)


def _publish(tag: str, args: argparse.Namespace, *, do_push: bool) -> None:
    _log(f"Publishing {tag}...")
    tag_merged_release(tag, message=args.message, push=do_push)
    if args.notes_file:
        notes_path = args.notes_file
        temporary_notes = None
    else:
        notes_path, temporary_notes = _notes_path_from_merged_main(tag)
    try:
        if not args.skip_gh:
            if do_push:
                _wait_then_publish(tag, notes_path, wait_ci=args.wait_ci)
            else:
                _log("==> Skipping GitHub release publish (--no-push).")
        else:
            _log("==> Skipping GitHub release publish (--skip-gh).")
    finally:
        if temporary_notes:
            temporary_notes.unlink(missing_ok=True)
    _log(f"Done. Published {tag}" if do_push else f"Done. Tagged {tag} locally (--no-push).")


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
            action = "ship" if args.ship else "prepare"
            print(
                f"dry-run: {action} {args.version} -> {new} (v{new}) "
                f"channel={args.channel or track_for_version(new)}"
            )
        else:
            print("error: version required", file=sys.stderr)
            return 1
        return 0

    if args.publish:
        tag = args.tag or tag_for_version(read_version())
        if not tag.startswith("v"):
            tag = f"v{tag}"
        _publish(tag, args, do_push=do_push)
        _cleanup_finished_release_branch(tag)
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
        branch = ensure_release_branch(tag, amend=True)
        _log(f"==> Preparing amended-notes PR on {branch}...")

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

        _log(f"Amended notes prepared for {tag}.")
        if do_push:
            _log(f"  Open PR: gh pr create --base main --head {branch} --title 'Amend release notes for {tag}'")
        _log(f"  After merge: gh release edit {tag} --notes-file {notes_path}")
        return 0

    if not args.version:
        print("error: version bump required (major, minor, patch, or X.Y.Z)", file=sys.stderr)
        return 1

    if args.ship and (not do_push or args.no_commit or args.skip_gh):
        raise ReleaseError("--ship requires commit, push, and GitHub publishing to be enabled")

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
    branch = ensure_release_branch(tag)
    _log(f"==> Preparing release PR on {branch}...")

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
        target_version=new,
        channel=args.channel,
        commit=not args.no_commit,
        push=do_push,
        message=args.message,
        notes_content=content,
        notes_path=notes_path,
        check_tag=False,  # already verified above
    )
    _log(f"  Release PR is ready: {result.old_version} -> {result.new_version} ({result.tag})")

    _log(f"\nRelease {tag} prepared.")
    _log(f"  Notes: {notes_path}")
    if do_push:
        pr_url = ensure_release_pr(tag, branch)
        pr_head_commit = head_commit()
    else:
        _log(f"  Push branch later: git push -u origin {branch}")
        pr_url = None
        pr_head_commit = None

    should_ship = bool(args.ship)
    if pr_url and not should_ship:
        should_ship = _confirm_ship(tag, pr_url)
    if pr_url and should_ship:
        assert pr_head_commit is not None
        merge_release_pr(pr_url, head_commit=pr_head_commit)
        _publish(tag, args, do_push=True)
        _cleanup_finished_release_branch(tag, branch)
        return 0

    if pr_url:
        _log(f"  Release PR: {pr_url}")
    _log(f"  After merge: ./scripts/release.sh --publish --tag {tag}")
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
