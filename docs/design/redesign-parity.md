# PA redesign parity matrix

Baseline: `6be1c39b2bc5d1269ee374d618934872b0c165c1`. Inventory captured before implementation. Approved design: card b6e6495f-c6c3-4393-b7ae-a97dcd8606f1, v3 attachment d1fb25fa24aa48d1a55130f9386de1a8 (aed217751db0add0bc62255c0ea4f4ee5f3823bc7a5ad6c05a8688382b48a08c); supporting v2 guidelines f28da29c449941ee8fc1e79fdcde310f.

Status: implementation in progress. Retaining a template/handler is source parity, not browser acceptance. Every row requires runtime verification before broad completion.

| Existing feature | Current entry/behavior | Redesigned successor | Verification | Status / gap |
|---|---|---|---|---|
| Home attention, motion, outcomes | /; partials/home/sections.html | Same route and live sections | test_home_attention_queue, test_home_lifecycle_evidence | Retained; visual acceptance pending |
| Global Work and capture | /work; optional project; state/lane boards, saved views, search, facets, pagination, drag/drop | Work rail destination; existing boards and forms retained | test_work_facets, test_work_board_by_state, test_work_discovery_browser | Retained; global list and No project filter pending |
| Card details, materials, completion | partials/card-detail*; tabs; edit/share/source/download/revisions; assignment, dispatch defaults; acceptance separate from lane | Same dialogs and URLs | test_card_work_ux, card attachments and completion tests | Retained; browser parity pending |
| Goals | /goals; packages, retry, lifecycle | Goals rail destination | goals tests | Retained; visual acceptance pending |
| Projects and repositories | /projects; sticky count tabs, settings dialog, repositories, worker groups, defaults, PRs | Same project navigation, scoped Work | test_projects_workspace_browser, test_projects_accessibility | Preserve merged #768; runtime verification pending |
| Pull requests | /pull-requests; durable watches, evidence, review and merge state | Registered navigation retained | pr supervisor tests | Retained |
| Conversation selection and history | /agent; new/select/branch/thread/archive/search; deep links and recovery | Conversations rail destination; original workspace | test_chat_shell, test_agent_history_sidebar, navigation browser tests | Retained |
| Live conversation control | agent-chat.js; streaming, queue/steering/cancel, drafts, agent/model/effort/permission selectors | Original chat widgets and handler lifecycle | conversation browser tests, agent chat tests | Retained; active session browser acceptance pending |
| Conversation outputs and interactions | tools/Terminal; issues/start; permissions/questions; attachments/resources; visualizations/results | Original tool views, dialogs and preview sandbox | test_conversation_work_browser, visualizations/file viewer tests | Retain #769; original chat acceptance remains separate |
| Loops and reminders | /loops; scheduling.js; session-reminders.js | Loops rail destination; same forms | loop and reminder tests | Retained |
| Reports and results | /reports; telemetry; completion-results and material-history deep links | Same pages and chrome telemetry links | telemetry and completion records tests | Retained |
| Memory and knowledge | /knowledge, /memory-audit; knowledge partials; browse files | All registered links retained | knowledge and memory tests | Retained |
| Workshop | /workshop; floor and compact views; live stream | Workshop rail destination | workshop browser/model tests | Retained |
| Fleet and operations | /fleet; realms, instances, remote operations, workers, health, backups | Fleet rail destination; original sections | fleet tests | Retained; responsive acceptance pending |
| Settings and configuration | /settings; appearance, agent, MCP, GitHub scope, prompts, telemetry, config, backups, instance | Settings at rail bottom; all sections retained; density under Appearance | test_themes, configuration tests | Density added; remaining settings unchanged |
| Appearance | Theme catalog; system/light/dark; cookies, local storage, personal preferences | Same themes; theme icon directly below Settings | test_themes; redesign browser regression | Retained; density preference is per browser |
| Account and notifications | Sign in/out, next URL, account, notifications filters/read/actions/pagination; agent status | Original chrome actions retained | test_auth_navigation, test_auth_navigation_browser, notifications tests | Retained |
| Navigation and accessibility | HTMX app-view swaps; shareable URLs; history/focus/scroll; CSRF; mobile menu | Persistent rail desktop, accessible menu narrow; same route manager | test_browser_navigation_readiness; redesign browser regression | Keyboard, touch and zoom verification pending |

## Source inventory

The following inventory preserves entry points omitted from the mockup. No registered route, form or handler is intentionally removed. Recent integration: #767 checkbox sizing, #768 Projects, #769 streaming preview/file viewer. Agent/model/permission defaults and data schemas are unchanged.

### Page templates
- `src/pa/server/templates/pages/agent.html`: `/agent?new=1`
- `src/pa/server/templates/pages/browse.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/card-material-history.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/completion-results.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/fleet.html`: `fit`; `reset`; `zoom-in`; `zoom-out`
- `src/pa/server/templates/pages/goals.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/home.html`: `/agent?new=1`; `/partials/home/sections?realm={{ active_realm }}`
- `src/pa/server/templates/pages/knowledge.html`: `/knowledge`; `/partials/knowledge`; `/partials/knowledge/bulk`
- `src/pa/server/templates/pages/login.html`: `/login`
- `src/pa/server/templates/pages/loops.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/memory-audit.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/pr-supervisor.html`: `/pull-requests`
- `src/pa/server/templates/pages/projects.html`: `/partials/projects/{{ project.id }}/pr-policy?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `/projects`; `/projects/repositories/{{ repository.id }}/checkout/remove?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `/projects/repositories/{{ repository.id }}/checkout?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `/projects/repositories/{{ repository.id }}/delete?realm={{ active_realm }}&view=repos`; `/projects/repositories/{{ repository.id }}?realm={{ active_realm }}&view=repos&repository={{ repository.id }}`; `/projects/repositories?realm={{ active_realm }}&view=repos&project={{ selected_project }}`; `/projects/{{ project.id }}/repositories/{{ repository.id }}/unlink?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `/projects/{{ project.id }}/repositories?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `/projects/{{ project.id }}/worker-default?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `/projects/{{ project.id }}?realm={{ active_realm }}&project={{ project.id }}&tab={{ project_tab }}`; `{{ tab.url }}`
- `src/pa/server/templates/pages/reports.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/settings.html`: server-rendered page; delegated actions
- `src/pa/server/templates/pages/work.html`: `/partials/cards?{{ filter_query }}{% if filter_query %}&{% endif %}attention={{ bucket.value }}`; `/partials/cards?{{ filter_query }}{% if filter_query %}&{% endif %}lane={{ lane.value }}`; `/work`; `/work?realm={{ active_realm }}`; `/work?realm={{ active_realm }}&amp;blocked=blocked`; `/work?realm={{ active_realm }}&amp;group=lane`; `/work?realm={{ active_realm }}&amp;lane=done&amp;updated=7`; `/work?realm={{ active_realm }}&amp;lane=waiting`; `/work?realm={{ active_realm }}&amp;owner={{ principal_id | default('') }}&amp;blocked=unblocked`; `/work?{{ view.query }}`; `{{ chip.remove_url }}`
- `src/pa/server/templates/pages/workshop.html`: server-rendered page; delegated actions

### Behavior modules
- `account.js`
- `agent-chat-draft-widget.js`
- `agent-chat-drafts.js`
- `agent-chat.js`
- `agent-terminal.js`
- `card-attachments.js`
- `chat-format.js`
- `chat-modal.js`
- `composer-triggers.js`
- `configuration.js`
- `conversation-issues.js`
- `conversation-threads.js`
- `csrf.js`
- `execution-selection.js`
- `file-browser.js`
- `fleet.js`
- `github-scope.js`
- `goals.js`
- `history-policy.js`
- `instance-identity.js`
- `layout.js`
- `native-effort.js`
- `navigation.js`
- `notifications.js`
- `repository-dialog.js`
- `session-recovery.js`
- `session-reminders.js`
- `spa.js`
- `telemetry.js`
- `theme.js`
- `visualizations.js`
- `work-filters.js`
- `workshop-floor-model.js`
- `workshop-floor-stream.js`
- `workshop.js`

## Completion gates

Run relevant tests, real fixture browser coverage (desktop, narrow, 200% zoom, dark/light, keyboard, long/empty states), then ready PR and durable exact-head supervision. Merged source, active runtime rollout and original-chat acceptance are separate. Do not mark this global redesign complete from a shell slice.

## Slice 1 verification

Implemented shared workspace rail, preserved registered navigation, Settings/theme placement, 32 px desktop and 44 px touch geometry, 16/12 px gutters and browser-persisted density. Added `tests/test_redesign_shell_browser.py`: actual HTMX shell navigation, mixed project/unassigned global counts, persistent density, dark/light, 1440/720/390/320 widths, Escape and focus restoration. Desktop and narrow screenshots inspected. Half-width viewport tests reflow; native browser 200% zoom remains an explicit acceptance gap.

No backend workflows, agent/model/permission defaults or data migrations changed. All existing forms, card views, project tabs and chat widget handlers remain in place. Full per-destination visual/action acceptance and global list design are subsequent work, not completed by these source-retention checks.
