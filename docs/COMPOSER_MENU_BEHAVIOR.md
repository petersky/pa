# Composer and pop-up menu behavior

Transient pop-up menus dismiss when clicking elsewhere, including another menu.
Shared shell handling covers chat settings, session actions, send options, card
actions, account, and responsive navigation. It runs in capture phase so links
and controls that stop propagation still dismiss menus. It also handles menus
inserted by HTMX without rebinding. Escape closes these menus; keyboard focus
returns to the summary when focus was inside the menu.

Notifications and work-tag suggestions retain their own outside-click and Escape
handling. Native select menus retain browser dismissal behavior. Composer command,
skill, and mention suggestions close outside the input/menu, preserving clicks
inside the input and suggestion selection.

Conversation terminal, tool, and plan flyouts deliberately remain open: these are
reference panels users read while composing or inspecting messages, with explicit
close controls. Inline details (including diagnostics, evidence, session details,
filters, and advanced settings) also remain expanded while interacting elsewhere;
they are document disclosures rather than transient pop-up menus. No transient
menu is exempted from outside dismissal.

During an active turn the submit label is **Queue**. The stop control is a square
icon with accessible name **Stop response**, above Attach on the left of the input.
It keeps the existing current-response cancellation behavior and is hidden when
disabled. Draft persistence and status remain, without a Clear draft control.
The textarea grows to about six lines, then scrolls. Agent, model, and permission
defaults are unchanged.
