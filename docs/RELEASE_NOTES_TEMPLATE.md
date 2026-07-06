# Release Notes Template

Use this structure for every PA release. Replace placeholder sections with concrete content derived from the changelog. Omit empty sections.

---

# {{TAG}} — {{VERSION}}

**Release track:** {{CHANNEL}}  
**Date:** {{DATE}}

## Summary

One short paragraph: what this release is for and who should care.

## Highlights

- Bullet list of the 3–5 most important changes

## Added

- New features and capabilities

## Changed

- Behavior changes and improvements

## Fixed

- Bug fixes

## Upgrade

```bash
pa update
# or fresh install:
curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

## Changelog

<!-- Below is the raw git log since the previous tag. Summarize above; keep this section as a concise commit list. -->

{{CHANGELOG}}
