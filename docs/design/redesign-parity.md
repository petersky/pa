# PA redesign parity matrix

Baseline: `6be1c39b2bc5d1269ee374d618934872b0c165c1`. Inventory captured before implementation. Approved design: card b6e6495f-c6c3-4393-b7ae-a97dcd8606f1, v3 attachment d1fb25fa24aa48d1a55130f9386de1a8 (aed217751db0add0bc62255c0ea4f4ee5f3823bc7a5ad6c05a8688382b48a08c); supporting v2 guidelines f28da29c449941ee8fc1e79fdcde310f.

Status: implementation in progress. Retaining a template/handler is source parity, not browser acceptance. Every row requires runtime verification before broad completion.

| Existing feature | Current entry/behavior | Redesigned successor | Verification | Status / gap |
|---|---|---|---|---|
| Home attention, motion, outcomes | /; partials/home/sections.html | Same route and live sections | test_home_attention_queue, test_home_lifecycle_evidence | Isolated destination/layout checks passed; active runtime acceptance pending |
| Global Work and capture | /work; optional project; state/lane boards, saved views, search, facets, pagination, drag/drop | Work rail destination; existing boards and forms retained | test_work_facets, test_work_board_by_state, test_work_discovery_browser | Global list and No project filter implemented; targeted real-app verification passed; PR integration pending |
| Card details, materials, completion | partials/card-detail*; tabs; edit/share/source/download/revisions; assignment, dispatch defaults; acceptance separate from lane | Same dialogs and URLs | test_card_work_ux, card attachments and completion tests | Existing material/detail regressions passed; active runtime acceptance pending |
| Goals | /goals; packages, retry, lifecycle | Goals rail destination | goals tests | Isolated destination/layout checks passed; active runtime acceptance pending |
| Projects and repositories | /projects; sticky count tabs, settings dialog, repositories, worker groups, defaults, PRs | Same project navigation, scoped Work | test_projects_workspace_browser, test_projects_accessibility | Merged #768 preserved; project browser actions passed; active runtime acceptance pending |
| Pull requests | /pull-requests; durable watches, evidence, review and merge state | Registered navigation retained | pr supervisor tests | Retained |
| Conversation selection and history | /agent; new/select/branch/thread/archive/search; deep links and recovery | Conversations rail destination; original workspace | test_chat_shell, test_agent_history_sidebar, navigation browser tests | Retained |
| Live conversation control | agent-chat.js; streaming, queue/steering/cancel, drafts, agent/model/effort/permission selectors | Original chat widgets and handler lifecycle | conversation browser tests, agent chat tests | Conversation chrome/reentry/queue/browser actions passed; original live-chat acceptance pending |
| Conversation outputs and interactions | tools/Terminal; issues/start; permissions/questions; attachments/resources; visualizations/results | Original tool views, dialogs and preview sandbox | test_conversation_work_browser, visualizations/file viewer tests | Retain #769; original chat acceptance remains separate |
| Loops and reminders | /loops; scheduling.js; session-reminders.js | Loops rail destination; same forms | loop and reminder tests | Retained |
| Reports and results | /reports; telemetry; completion-results and material-history deep links | Same pages and chrome telemetry links | telemetry and completion records tests | Retained |
| Memory and knowledge | /knowledge, /memory-audit; knowledge partials; browse files | All registered links retained | knowledge and memory tests | Retained |
| Workshop | /workshop; floor and compact views; live stream | Workshop rail destination | workshop browser/model tests | Retained |
| Fleet and operations | /fleet; realms, instances, remote operations, workers, health, backups | Fleet rail destination; original sections | fleet tests | Desktop/narrow destination checks passed; active runtime acceptance pending |
| Settings and configuration | /settings; appearance, agent, MCP, GitHub scope, prompts, telemetry, config, backups, instance | Settings at rail bottom; all sections retained; density under Appearance | test_themes, configuration tests | Density added; remaining settings unchanged |
| Appearance | Theme catalog; system/light/dark; cookies, local storage, personal preferences | Same themes; theme icon directly below Settings | test_themes; redesign browser regression | Retained; density preference is per browser |
| Account and notifications | Sign in/out, next URL, account, notifications filters/read/actions/pagination; agent status | Original chrome actions retained | test_auth_navigation, test_auth_navigation_browser, notifications tests | Retained |
| Navigation and accessibility | HTMX app-view swaps; shareable URLs; history/focus/scroll; CSRF; mobile menu | Persistent rail desktop, accessible menu narrow; same route manager | test_browser_navigation_readiness; redesign browser regression | Keyboard/touch, page scale 2 and half-width reflow passed in fixtures; active runtime acceptance pending |

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

## Slice 2: global Work list

The rail Work link opens `/work?view=list`; `/work` and explicit state/lane board URLs retain their existing behavior. List uses the same real card presenters, contextual actions, dialogs and bounded projection queries. Project `none` means unassigned within the selected realm; no schema migration. Project selection remains optional. All project, kind, owner, instance, lifecycle, blocked, tag and updated filters remain; list adds lane selection. List pagination retains expanded rows during live refresh, and saved views now retain view/group/lane instead of dropping those parameters.

Validation: `tests/test_global_work_list.py` checks mixed assigned/unassigned counts, realm isolation, filtered pagination, saved URL parameters, actual HTMX filters, expanded refresh, card dialog/history, switching back to lane board, and 1440/720/390/320 widths. The unit test, real-app browser test and shared-shell browser test passed together. Work facet regressions passed. Desktop and phone list screenshots inspected; corrected a CSS specificity issue so Work uses the shared gutter and adjusted the four-control filter row.

Remaining: representative Home/conversation styling and remaining destination visual/action acceptance, native 200% browser zoom, error/long/stale states, runtime rollout and original-chat acceptance. The inventory remains open until those checks close.

## Slice 3: Home/conversation and action verification

Home keeps all real queue/outcome/health sections and presents attention + motion in the wide column, outcomes + health alongside. Narrow layouts use one column; the existing Home keyboard/dialog/focus and narrow/zoom regression passed after fixing a specificity conflict. Conversation chrome shares desktop/touch heights and compact spacing, without replacing the widget, menus, selectors or transport handlers. Removed the redundant Activity/inspection subtitle; meaningful input labels, recovery and provenance remain.

Twelve existing browser action tests passed together: queue/steering controls (`test_agent_queue_browser.py`), thread/branch navigation (`test_chat_threads_browser.py`), details/tool panels (`test_chat_detail_tabs_browser.py`), reminders/Loops (`test_loops_browser.py`), notifications (`test_notifications_browser.py`), results (`test_completion_records_browser.py`), attachments (`test_card_attachments_browser.py`), and project tabs/settings (`test_projects_workspace_browser.py`). Initial real conversation and chat-reentry browser checks passed in the Home/conversation run; 41 tests passed and one Home layout check failed, then passed after the specificity correction.

### Control-level preservation ledger

These controls retain their existing handlers or templates; the verification column names concrete regression sources. A passed suite is distinguished from the remaining end-to-end acceptance work above.

| Workflow/control | Current and redesigned successor | Verification | Status |
|---|---|---|---|
| Create/select/archive/search conversations | `/agent`, session history sidebar and Session menu | `test_chat_shell.py`, `test_agent_history_sidebar.py`, `test_conversation_chrome_browser.py` | Retained; real conversation chrome passed |
| Branch/parallel thread and parent history | Original Branch tab and thread navigation | `test_chat_threads_browser.py` | Browser actions passed |
| Streaming/reentry/history/drafts and recovery | Original `agent-chat.js`, draft stores, recovery controls | `test_agent_chat_reentry_browser.py`, `test_agent_chat_drafts.py`, `test_agent_chat_stream_join.py` | Reentry browser passed; source handlers unchanged |
| Queue/steer/cancel | Original composer/queue controls | `test_agent_queue_browser.py`, `test_agent_chat_queue_stop.py` | Queue browser passed |
| Agent/model/effort/permission/defaults | Original settings and execution-selection forms | `test_agent_effort_browser.py`, `test_native_effort.py`, `test_purpose_permission_defaults.py` | Retained; no default changes |
| Tools/Terminal/plans | Original named activity controls and tabs | `test_chat_detail_tabs_browser.py`, `test_conversation_titles_terminal.py` | Details browser passed; Terminal handler unchanged |
| Issues/start workflows and correlated questions | Original Issues tab and interactions | `test_conversation_work_browser.py`, `test_goal_session_permissions.py` | Issues workflow browser and permission-policy regressions passed; active runtime acceptance pending |
| File/resource attachments, edit/source/download/history | Original card material dialogs and revision routes | `test_card_attachments_browser.py`, `test_attachment_materialization_api.py` | Attachment browser passed |
| Visualization/media previews and results | Original sandbox renderer and results route | `test_visualization_chat_browser.py`, `test_completion_records_browser.py` | Results browser passed; #769 retained |
| Capture/edit/assign/optional project | Original New card/detail forms; global list adds No project | `test_card_work_ux.py`, `test_global_work_list.py` | Global fixture/unit/browser checks passed |
| State/lane boards, filters/saved views/pagination | `/work`, List/By state/By lane; original facets | `test_work_discovery_browser.py`, `test_work_facets.py`, `test_work_board_by_state.py` | Passed; 1024 px root correction uses container width |
| Completion/evidence/acceptance, distinct from lane | Original detail/result controls | `test_completion_records_browser.py`, `test_completion_requirements.py`, `test_auth_navigation_browser.py` | Results and signed-in acceptance journey passed |
| Project scoped work/repos/worker defaults/PR tabs | Original sticky project navigation and settings dialog | `test_projects_workspace_browser.py`, `test_projects_accessibility.py` | Browser passed; #768 retained |
| Loops/reminders scheduling and failure recovery | Original Loops editor and session reminders | `test_loops_browser.py`, `test_loops.py` | Both browser widths/actions passed |
| Notifications/account/sign-in/return URLs | Original persistent chrome and sign-in forms | `test_notifications_browser.py`, `test_auth_navigation_browser.py` | Browser actions passed |
| Settings/configuration/personal scope | Original section navigation and forms; density under Appearance | `test_settings_scope_browser.py`, `test_configuration_registry.py`, `test_themes.py` | Retained; density browser persistence passed |
| Goals/knowledge/memory/workshop/fleet/reporting | All registered destinations retained in rail | `test_durable_goals.py`, `test_knowledge_capture.py`, `test_limbic_memory.py`, `test_workshop_stream.py`, `test_telemetry.py` | Route/action sweep in progress; source tests retained |

Direct browser navigation in the broad destination sweep timed out at guest Loops and then signed-in Reports; direct authorized HTTP reads returned 200. A speculative Loops async-route change did not resolve the issue and was reverted. This remains a reported browser/navigation acceptance uncertainty, not a verified product repair. The sweep now exercises actual rail/HTMX routing separately. No broad completion or live rollout is claimed.

### Destination and lifecycle closure evidence

The signed-in real-app sweep passed every registered rail destination at 1440 and 390 px; all existing theme variants in light/dark; native page-scale 2 plus 720 px reflow; keyboard rail focus/Escape; and retained chat-widget identity through theme changes. This is browser page-scale + reflow testing, not a claim of controlling the browser's toolbar zoom setting. Long title fixtures exposed and corrected Workshop floor-card minimum sizing and Work project-label wrapping. A final Home-gutter assertion was added after screenshot review.

The direct-navigation uncertainty above was resolved in the deterministic regression: notification and Workshop event streams now close on pagehide and reconnect on persisted pageshow. The same sweep passes repeated full-document visits after this change; independent HTTP reads and HTMX routing had already passed. A dedicated notification lifecycle test verifies max one live source/poll timer through repeated restoration, zero after hide. Existing notification-browser and Workshop stream suites passed (16 tests). No server scheduling route change was retained. This is verified in isolated fixtures; production rollout/original-chat acceptance remains pending.

Shell PR #770 merged as `551a62835beb8341091b5c2f93225ba65673a416`. Global Work PR #771, including the 1024 px container-based board correction, merged as `b3c077c34701e280daf8839d85877548f7b856a8`, watched final head `ada81bf06dc455999e142d9479c42452d1c9699e`. Both had stable-green PA supervision and independent exact-head CI/review/thread/mergeability checks before merge.

Desktop chrome now includes the approved Search all work form. It submits only query/view/realm and synchronizes the realm from the current URL before submission; project scope is never inherited. Narrow layouts retain global search through the Work destination. The shared-shell browser regression searches from a project page in a separate research realm and verifies the unassigned matching card and absence of a project parameter. This, the sign-in/return-layout browser journey, and the all-destination/theme/direct-navigation sweep passed together after the header addition.

### Final selector, defaults and permission coverage

Twenty-three additional tests passed across `test_conversation_work_browser.py`, `test_agent_effort_browser.py`, `test_settings_scope_browser.py`, `test_purpose_permission_defaults.py` and `test_goal_session_permissions.py`. This closes the isolated fixture gaps for issues/start editing/conflict/retry, effort selection, personal versus instance settings scope, and goal/session permission boundaries. The original live conversation still requires post-rollout acceptance.

CI found two test fixture/selector assumptions after global search was added: the file-browser ASGI fixture lacked `query_string` (corrected; all 8 file-browser tests passed), and a Work filter test's unscoped `input[name=q]` selected the new global search instead of the local filter form (scoped to `.work-filters`; the full existing filter/history/touch browser regression passed). Product filtering semantics were retained.

All source changes remain subject to final PR #772 CI/review/merge. Active runtime rollout and verification of the originating conversation are the remaining acceptance stages; no broad completion has been recorded.
