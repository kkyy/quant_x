"""
Push notification dispatcher.

Supported channels (configure in config/notify.yaml):
- Bark        iOS push app — https://bark.day.app
- PushPlus    WeChat push  — https://www.pushplus.plus
- DingTalk    钉钉群机器人
- Server酱    微信          — https://sct.ftqq.com

Enable a channel by setting enabled: true and filling in the credentials.
"""
from __future__ import annotations
import base64
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

_TIMEOUT = 12   # seconds


class NotificationPusher:

    def __init__(self, config: dict):
        self.cfg = config.get("notify", {})

    # ── public ────────────────────────────────────────────────────────────────

    def send(
        self,
        title: str,
        content: str,
        url: Optional[str] = None,
    ) -> Dict[str, bool]:
        """
        Send to all enabled channels.

        Returns:
            {channel_name: success_bool}
        """
        results: Dict[str, bool] = {}
        dispatchers = {
            "bark":       self._bark,
            "pushplus":   self._pushplus,
            "dingtalk":   self._dingtalk,
            "serverchan": self._serverchan,
        }
        for name, fn in dispatchers.items():
            if self.cfg.get(name, {}).get("enabled", False):
                results[name] = fn(title, content, url)

        if not results:
            logger.warning(
                "No notification channels enabled. "
                "Edit config/notify.yaml to enable at least one channel."
            )
        return results

    # ── Bark ─────────────────────────────────────────────────────────────────

    def _bark(self, title: str, content: str, url: Optional[str] = None) -> bool:
        cfg = self.cfg.get("bark", {})
        key = cfg.get("device_key", "")
        if not key:
            logger.error("bark.device_key not set")
            return False
        server = cfg.get("server_url", "https://api.day.app").rstrip("/")
        try:
            payload: Dict = {
                "title":    title,
                "body":     content[:1000],
                "sound":    "minuet",
                "badge":    1,
                "group":    "quant_signal",
                "autoCopy": 0,
            }
            if url:
                payload["url"] = url
            r = requests.post(f"{server}/{key}", json=payload, timeout=_TIMEOUT)
            r.raise_for_status()
            ok = r.json().get("code") == 200
            logger.info(f"Bark: {'ok' if ok else r.json()}")
            return ok
        except Exception as e:
            logger.error(f"Bark failed: {e}")
            return False

    # ── PushPlus ──────────────────────────────────────────────────────────────

    def _pushplus(self, title: str, content: str, _url=None) -> bool:
        token = self.cfg.get("pushplus", {}).get("token", "")
        if not token:
            logger.error("pushplus.token not set")
            return False
        try:
            # PushPlus renders markdown; replace newlines for better display
            md = content.replace("\n", "\n\n")
            r = requests.post(
                "http://www.pushplus.plus/send",
                json={"token": token, "title": title, "content": md, "template": "markdown"},
                timeout=_TIMEOUT,
            )
            r.raise_for_status()
            ok = r.json().get("code") == 200
            logger.info(f"PushPlus: {'ok' if ok else r.json()}")
            return ok
        except Exception as e:
            logger.error(f"PushPlus failed: {e}")
            return False

    # ── DingTalk ──────────────────────────────────────────────────────────────

    def _dingtalk(self, title: str, content: str, _url=None) -> bool:
        cfg = self.cfg.get("dingtalk", {})
        webhook = cfg.get("webhook_url", "")
        if not webhook:
            logger.error("dingtalk.webhook_url not set")
            return False
        try:
            url = webhook
            secret = cfg.get("secret", "")
            if secret:
                ts = str(round(time.time() * 1000))
                sign = base64.b64encode(
                    hmac.new(secret.encode(), f"{ts}\n{secret}".encode(), digestmod=hashlib.sha256).digest()
                ).decode()
                url = f"{webhook}&timestamp={ts}&sign={urllib.parse.quote_plus(sign)}"

            payload = {
                "msgtype": "markdown",
                "markdown": {"title": title, "text": f"# {title}\n\n{content}"},
            }
            r = requests.post(url, json=payload, timeout=_TIMEOUT)
            r.raise_for_status()
            ok = r.json().get("errcode") == 0
            logger.info(f"DingTalk: {'ok' if ok else r.json()}")
            return ok
        except Exception as e:
            logger.error(f"DingTalk failed: {e}")
            return False

    # ── Server酱 ──────────────────────────────────────────────────────────────

    def _serverchan(self, title: str, content: str, _url=None) -> bool:
        key = self.cfg.get("serverchan", {}).get("send_key", "")
        if not key:
            logger.error("serverchan.send_key not set")
            return False
        try:
            r = requests.post(
                f"https://sctapi.ftqq.com/{key}.send",
                data={"title": title, "desp": content},
                timeout=_TIMEOUT,
            )
            r.raise_for_status()
            ok = r.json().get("code") == 0
            logger.info(f"Server酱: {'ok' if ok else r.json()}")
            return ok
        except Exception as e:
            logger.error(f"Server酱 failed: {e}")
            return False
