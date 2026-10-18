# UX guidance

Make the next action and its result clear through the interface first. Before
adding instructions, improve labels, grouping, constraints, defaults, examples
and feedback. Apply this rule to the UI being changed; avoid unrelated rewrites.

Keep concise text beside a decision when it explains an input format, unusual
consequence, permission scope, destructive result, uncertainty or actionable
failure. Keep essential labels and consequences visible. Put supplementary help
behind one clearly named action, such as a native details control; never make it
hover-only. Keep frequent actions visible and give icons accessible, visible
labels when their meaning is needed to complete the task.

Prefer “Save my defaults” with an adjacent scope indication over instructions
explaining how to avoid changing instance defaults. Prefer separate work state,
result and next action over a paragraph explaining several meanings of “Done”.
Preserve provider-native permission terms and existing defaults, authorization
and completion rules when improving presentation.

## Review the journeys

Use an isolated PA instance with representative saved state. Do not dispatch real
work, change another person's defaults or manufacture production failures for a
usability check. Use the same fixture and starting point before and after a
change. Test desktop and a narrow touch layout, keyboard navigation and 200%
zoom. Include long labels and unavailable evidence.

| Task | What the user must be able to establish |
| --- | --- |
| Find the next required action | Who acts next, the visible action and any blocking consequence; start a saved request without inventing a new request identity. |
| Tell started from finished | Not started, queued, working, turn ended, delivered and accepted remain distinct. A source conversation, admitted request or merged PR alone does not prove completion. |
| Find delivered evidence | Locate the actual result or evidence reference and its attributable session/revision. A success badge or progress summary alone is insufficient. |
| Identify agent, model and permissions | Find the selected worker and native execution/permission mode; distinguish saved intent, preview and provider-confirmed effective settings. Unknown remains unknown. |
| Change a personal default | Identify “My defaults”, inspect the change and save it; verify instance/other-user values and permission approval settings did not change. |
| Distinguish stale machine data from failure | Find observation/last-success time and the latest attempt result. Retained stale values must not imply a current failure or current success. |

For each task record success or the precise remaining gap, first action, errors
and whether help was needed. Record these separately for each layout and each
version. Do not infer participant success or time savings from an expert walk-through.
If no UI change is proposed, record a baseline and explain why comparison is
unchanged or not applicable.

Check visible focus, logical Tab order, native details controls, tab arrow-key
navigation, dialog focus/return and touch reachability. Inspect accessible names,
selected/expanded states, status/alert regions and updates to their content.
Distinguish DOM/accessibility-tree checks from listening with a named screen
reader/browser combination; record an untested combination as untested. At 200%
zoom check readable labels, consequences, errors and actions without clipping.

## Evidence for a change

Link a short verification record from the PR: source/head revisions, fixture,
viewports, tasks, observations, regressions and limitations. Keep sanitized
screenshots or bounded observations; exclude credentials and conversation dumps.
Run behavior regressions for any affected boundary, especially personal save
isolation, permission scope and completion acceptance. Documentation and copy
checks complement practical PA browser verification.

The initial pa-ux-06 check is a heuristic review, not a user study or a measured
usability improvement. See [its verification record](validation/pa-ux-06.md).

## Sources

- [DWP hint text](https://design-system.dwp.gov.uk/guidance/hint-text): improve the question first; keep decision help brief.
- [GOV.UK buttons](https://design-system.service.gov.uk/components/button/): use action labels that describe the result.
- [GOV.UK details](https://design-system.service.gov.uk/components/details/): reserve disclosure for supplementary information.
- [WCAG labels or instructions](https://www.w3.org/WAI/WCAG22/Understanding/labels-or-instructions.html): provide labels and necessary input guidance.
