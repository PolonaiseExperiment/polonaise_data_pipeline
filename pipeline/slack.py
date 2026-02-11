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

    def __init__(self, webhook_url: Optional[str]):
        """Initialize notifier.

        Args:
            webhook_url: Slack webhook URL. If None, notifications are disabled.
        """
        self.webhook_url = webhook_url
        self.enabled = webhook_url is not None and len(webhook_url) > 0

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
            with urllib.request.urlopen(req, timeout=30) as response:
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
            "Pipeline Sync Started",
            f"Processing {file_count} new/modified files"
        )

    def notify_sync_complete(
        self,
        transferred: int,
        failed: int,
        bytes_transferred: int,
        duration_seconds: float
    ) -> bool:
        """Notify that sync completed."""
        mb = bytes_transferred / (1024 * 1024)
        speed = mb / duration_seconds if duration_seconds > 0 else 0

        if failed == 0:
            return self.send_success(
                "Pipeline Sync Complete",
                f"*Transferred:* {transferred} files\n"
                f"*Data:* {mb:.1f} MB\n"
                f"*Speed:* {speed:.1f} MB/s\n"
                f"*Duration:* {duration_seconds:.0f}s"
            )
        else:
            return self.send_warning(
                "Pipeline Sync Complete (with errors)",
                f"*Transferred:* {transferred} files\n"
                f"*Failed:* {failed} files\n"
                f"*Data:* {mb:.1f} MB\n"
                f"*Duration:* {duration_seconds:.0f}s"
            )

    def notify_connection_error(self, error: str) -> bool:
        """Notify about connection error."""
        return self.send_error(
            "Pipeline Connection Error",
            f"*Error:* {error}\n\nSync cannot proceed."
        )

    def notify_checksum_mismatch(self, file_path: str, attempt: int) -> bool:
        """Notify about checksum mismatch."""
        return self.send_warning(
            "Checksum Mismatch",
            f"*File:* {file_path}\n"
            f"*Attempt:* {attempt}\n"
            f"Remote checksum doesn't match local. Will retry."
        )

    def notify_verification_complete(
        self,
        total_files: int,
        verified_ok: int,
        mismatches: int,
        errors: int
    ) -> bool:
        """Notify about verification results."""
        if mismatches == 0 and errors == 0:
            return self.send_success(
                "Verification Complete",
                f"*Files checked:* {total_files}\n"
                f"*All checksums verified OK*"
            )
        else:
            return self.send_error(
                "Verification Found Issues",
                f"*Files checked:* {total_files}\n"
                f"*OK:* {verified_ok}\n"
                f"*Mismatches:* {mismatches}\n"
                f"*Errors:* {errors}"
            )
