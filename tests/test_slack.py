"""
Tests for Slack notification module.

Author: tunnell (https://github.com/tunnell)
"""

import pytest

from pipeline.slack import SlackNotifier, SlackMessage


class TestSlackNotifier:
    """Tests for SlackNotifier class."""

    def test_disabled_when_no_webhook(self):
        """Test notifier is disabled without webhook."""
        notifier = SlackNotifier(None)
        assert notifier.enabled is False

        # Should return False without error
        result = notifier.send_text("test")
        assert result is False

    def test_disabled_when_empty_webhook(self):
        """Test notifier is disabled with empty webhook."""
        notifier = SlackNotifier("")
        assert notifier.enabled is False

    def test_enabled_with_webhook(self):
        """Test notifier is enabled with webhook."""
        notifier = SlackNotifier("https://hooks.slack.com/services/xxx")
        assert notifier.enabled is True

    def test_message_creation(self):
        """Test SlackMessage creation."""
        msg = SlackMessage(
            text="Test message",
            color="#ff0000",
            title="Test Title"
        )

        assert msg.text == "Test message"
        assert msg.color == "#ff0000"
        assert msg.title == "Test Title"

    def test_message_defaults(self):
        """Test SlackMessage default values."""
        msg = SlackMessage(text="Just text")

        assert msg.text == "Just text"
        assert msg.color is None
        assert msg.title is None


class TestSlackNotifierMethods:
    """Tests for SlackNotifier helper methods.

    Note: These don't actually send to Slack, they just verify
    the methods exist and work with disabled notifier.
    """

    @pytest.fixture
    def notifier(self):
        """Create a disabled notifier for testing."""
        return SlackNotifier(None)

    def test_send_text(self, notifier):
        """Test send_text method exists."""
        result = notifier.send_text("test")
        assert result is False  # Disabled

    def test_send_success(self, notifier):
        """Test send_success method exists."""
        result = notifier.send_success("Title", "Text")
        assert result is False

    def test_send_warning(self, notifier):
        """Test send_warning method exists."""
        result = notifier.send_warning("Title", "Text")
        assert result is False

    def test_send_error(self, notifier):
        """Test send_error method exists."""
        result = notifier.send_error("Title", "Text")
        assert result is False

    def test_send_info(self, notifier):
        """Test send_info method exists."""
        result = notifier.send_info("Title", "Text")
        assert result is False

    def test_notify_sync_started(self, notifier):
        """Test sync started notification."""
        result = notifier.notify_sync_started(10)
        assert result is False

    def test_notify_sync_complete(self, notifier):
        """Test sync complete notification."""
        result = notifier.notify_sync_complete(
            transferred=5,
            failed=1,
            bytes_transferred=1024000,
            duration_seconds=30.5
        )
        assert result is False

    def test_notify_connection_error(self, notifier):
        """Test connection error notification."""
        result = notifier.notify_connection_error("SSH failed")
        assert result is False

    def test_notify_checksum_mismatch(self, notifier):
        """Test checksum mismatch notification."""
        result = notifier.notify_checksum_mismatch("file.tdms", 2)
        assert result is False

    def test_notify_verification_complete(self, notifier):
        """Test verification complete notification."""
        result = notifier.notify_verification_complete(
            total_files=100,
            verified_ok=98,
            mismatches=1,
            errors=1
        )
        assert result is False
