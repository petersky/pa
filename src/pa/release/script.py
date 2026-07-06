"""CLI entry point for scripts/release.sh."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pa.release.notes import (
    changelog_since,
    generate_release_notes,
    latest_tag,
    notes_path_for_tag,
    previous_tag,
    write_release_notes,
)
from pa.release.runner import (
    amend_release_notes,
    create_release,
    publish_github_release,
    resolve_version,
)
from pa.release.version import read_version, tag_for_version, track_for_version


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
    parser.add_argument("--tag", help="Tag to amend (default: latest)")
    parser.add_argument("--agent", dest="agent_cmd", help="Agent command")
    parser.add_argument("--agent-args", dest="agent_args", help="Agent arguments")
    parser.add_argument("--push", action="store_true", help="Push commit and tag")
    parser.add_argument("--no-commit", action="store_true", help="Skip git commit")
    parser.add_argument("--no-agent", action="store_true", help="Skip agent; use prefilled template")
    parser.add_argument("--notes-file", type=Path, help="Override release notes path")
    parser.add_argument("-m", "--message", help="Commit/tag message")
    parser.add_argument("--skip-gh", action="store_true", help="Skip gh release create/edit")
    parser.add_argument("--wait-ci", type=int, default=30, help="Seconds to wait for CI before gh edit")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    return parser.parse_args(argv)


def _tag_version(tag: str) -> str:
    return tag[1:] if tag.startswith("v") else tag


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if args.dry_run:
        if args.amend:
            tag = args.tag or latest_tag() or "?"
            print(f"dry-run: amend {tag}")
        elif args.version:
            new = resolve_version(args.version)
            print(f"dry-run: {args.version} -> {new} (v{new}) channel={args.channel or track_for_version(new)}")
        else:
            print("error: version required", file=sys.stderr)
            return 1
        return 0

    if args.amend:
        tag = args.tag or latest_tag()
        if not tag:
            print("error: no tag to amend", file=sys.stderr)
            return 1
        if not tag.startswith("v"):
            tag = f"v{tag}"
        version = _tag_version(tag)
        channel = args.channel or track_for_version(version)
        prev = previous_tag(tag)
        print(f"Amending release notes for {tag}...")

        content = generate_release_notes(
            version=version,
            tag=tag,
            channel=channel,
            since_tag=prev,
            use_agent=not args.no_agent,
            agent_cmd=args.agent_cmd,
            agent_args=args.agent_args,
        )
        notes_path = write_release_notes(tag, content, path=args.notes_file)
        print(f"Wrote {notes_path}")

        amend_release_notes(
            tag,
            content,
            commit=not args.no_commit,
            push=args.push,
            message=args.message,
            notes_path=notes_path,
        )

        if not args.skip_gh:
            try:
                publish_github_release(tag, notes_path, amend=True)
            except RuntimeError as exc:
                print(f"warning: gh release edit failed: {exc}", file=sys.stderr)

        print(f"Done. Amended {tag}")
        return 0

    if not args.version:
        print("error: version bump required (major, minor, patch, or X.Y.Z)", file=sys.stderr)
        return 1

    old = read_version()
    new = resolve_version(args.version)
    tag = tag_for_version(new)
    channel = args.channel or track_for_version(new)
    prev = latest_tag()

    print(f"Releasing {tag} (track: {channel})...")
    content = generate_release_notes(
        version=new,
        tag=tag,
        channel=channel,
        since_tag=prev,
        use_agent=not args.no_agent,
        agent_cmd=args.agent_cmd,
        agent_args=args.agent_args,
    )
    notes_path = write_release_notes(tag, content, path=args.notes_file)
    print(f"Wrote {notes_path}")

    result = create_release(
        args.version,
        channel=args.channel,
        commit=not args.no_commit,
        push=args.push,
        message=args.message,
        notes_content=content,
        notes_path=notes_path,
    )
    print(f"{result.old_version} -> {result.new_version} ({result.tag})")

    if not args.skip_gh:
        if args.push and args.wait_ci > 0:
            print(f"Waiting {args.wait_ci}s for CI...")
            time.sleep(args.wait_ci)
        try:
            publish_github_release(tag, notes_path, amend=False)
        except RuntimeError as exc:
            print(f"warning: gh release publish failed: {exc}", file=sys.stderr)

    print(f"\nRelease {tag} complete.")
    print(f"  Notes: {notes_path}")
    if not args.push:
        print("  Run with --push to publish tag and trigger CI.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
