# Agent Instructions (excel2sql)

- After completing each user-requested task (right before sending the final response), trigger a macOS system notification with the exact text `任务已完成`.
  - Preferred (JXA), since some environments/locales fail to parse the AppleScript form:
    - `osascript -l JavaScript -e 'var app = Application.currentApplication(); app.includeStandardAdditions = true; app.displayNotification("任务已完成", {withTitle: "excel2sql"});'`
  - Alternate (AppleScript):
    - `osascript -e 'display notification "任务已完成" with title "excel2sql"'`
  - If the environment blocks GUI/system notification commands, request the required approval/escalation and then retry.
- Do not close the terminal or end the Codex session automatically after the notification; keep the session open for follow-up tasks.
