"""
Slack notification module.

Author: tunnell (https://github.com/tunnell)
"""

import json
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass


@dataclass
class SlackMessage:
    """A Slack message with optional formatting."""
    text: str
    color: Optional[str] = None  # Hex color for attachment
    title: Optional[str] = None


class SlackNotifier:
    """Sends notifications to Slack."""

    def __init__(self, webhook_url: Optional[str], timeout: int = 30,
                 run_label: str = ""):
        """Initialize notifier.

        Args:
            webhook_url: Slack webhook URL. If None, notifications are disabled.
            timeout: HTTP request timeout in seconds.
            run_label: Run identifier (e.g. "run45") included in all messages.
        """
        self.webhook_url = webhook_url
        self.timeout = timeout
        self.run_label = run_label
        self.enabled = webhook_url is not None and len(webhook_url) > 0

    def _title(self, title: str) -> str:
        """Prefix a title with the run label if set."""
        if self.run_label:
            return f"[{self.run_label}] {title}"
        return title

    def send(self, message: SlackMessage) -> bool:
        """Send a message to Slack.

        Returns True on success, False on failure or if disabled.
        """
        if not self.enabled:
            return False

        if message.color:
            payload = {
                "attachments": [{
                    "color": message.color,
                    "title": message.title or "Pipeline",
                    "text": message.text,
                    "ts": datetime.now().timestamp()
                }]
            }
        else:
            payload = {"text": message.text}

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"}
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                return response.status == 200
        except urllib.error.URLError:
            return False

    def send_text(self, text: str) -> bool:
        """Send a simple text message."""
        return self.send(SlackMessage(text=text))

    def send_success(self, title: str, text: str) -> bool:
        """Send a success message (green)."""
        return self.send(SlackMessage(text=text, color="#36a64f", title=title))

    def send_warning(self, title: str, text: str) -> bool:
        """Send a warning message (orange)."""
        return self.send(SlackMessage(text=text, color="#FFA500", title=title))

    def send_error(self, title: str, text: str) -> bool:
        """Send an error message (red)."""
        return self.send(SlackMessage(text=text, color="#ff0000", title=title))

    def send_info(self, title: str, text: str) -> bool:
        """Send an info message (blue)."""
        return self.send(SlackMessage(text=text, color="#439FE0", title=title))

    # Pre-built pipeline messages

    def notify_sync_started(self, file_count: int) -> bool:
        """Notify that sync has started."""
        return self.send_info(
            self._title("Sync Started"),
            f"Processing {file_count} new/modified files"
        )

    def notify_sync_complete(
        self,
        transferred: int,
        failed: int,
        bytes_transferred: int,
        duration_seconds: float,
        recent_summary: str = ""
    ) -> bool:
        """Notify that sync completed."""
        mb = bytes_transferred / (1024 * 1024)
        speed = mb / duration_seconds if duration_seconds > 0 else 0

        body = (
            f"*Transferred:* {transferred} files\n"
            f"*Data:* {mb:.1f} MB ({speed:.1f} MB/s)\n"
            f"*Duration:* {duration_seconds:.0f}s"
        )
        if failed > 0:
            body += f"\n*Failed:* {failed} files"
        if recent_summary:
            body += f"\n\n{recent_summary}"

        if failed == 0:
            return self.send_success(self._title("Sync Complete"), body)
        else:
            return self.send_warning(self._title("Sync Complete (with errors)"), body)

    def notify_connection_error(self, error: str) -> bool:
        """Notify about connection error."""
        return self.send_error(
            self._title("Connection Error"),
            f"*Error:* {error}\n\nSync cannot proceed."
        )

    def notify_checksum_mismatch(self, file_path: str, attempt: int) -> bool:
        """Notify about checksum mismatch."""
        return self.send_warning(
            self._title("Checksum Mismatch"),
            f"*File:* {file_path}\n"
            f"*Attempt:* {attempt}\n"
            f"Remote checksum doesn't match local. Will retry."
        )

    def notify_verification_complete(
        self,
        total_files: int,
        verified_ok: int,
        mismatches: int,
        errors: int,
        recent_summary: str = ""
    ) -> bool:
        """Notify about verification results."""
        body = (
            f"*Files checked:* {total_files}\n"
            f"*OK:* {verified_ok}"
        )
        if mismatches > 0:
            body += f"\n*Mismatches:* {mismatches}"
        if errors > 0:
            body += f"\n*Errors:* {errors}"
        if recent_summary:
            body += f"\n\n{recent_summary}"

        if mismatches == 0 and errors == 0:
            return self.send_success(self._title("Verification Complete"), body)
        else:
            return self.send_error(self._title("Verification Found Issues"), body)
