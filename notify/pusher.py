"""
Push notification dispatcher.

Supported channels (configure in config/notify.yaml):
- Bark        iOS push app — https://bark.day.app
- PushPlus    WeChat push  — https://www.pushplus.plus
- DingTalk    钉钉群机器人
- Server酱    微信          — https://sct.ftqq.com
- WeChat MP   微信公众号模板消息 — https://mp.weixin.qq.com

Enable a channel by setting enabled: true and filling in the credentials.
"""
from __future__ import annotations
import base64
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_TIMEOUT = 12   # seconds
_WX_TOKEN_URL = "https://api.weixin.qq.com/cgi-bin/token"
_WX_SEND_URL  = "https://api.weixin.qq.com/cgi-bin/message/template/send"


class NotificationPusher:

    def __init__(self, config: dict):
        self.cfg = config.get("notify", {})
        # in-memory cache for WeChat access_token: {appid: (token, expires_at)}
        self._wx_token_cache: Dict[str, Tuple[str, float]] = {}

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
            "wechat_mp":  self._wechat_mp,
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

    # ── WeChat MP (微信公众号模板消息) ──────────────────────────────────────────

    def _wechat_mp(self, title: str, content: str, url: Optional[str] = None) -> bool:
        """
        Send a WeChat Official Account template message.

        Requires a verified service account (服务号) with template messaging enabled.
        Template should contain fields: first / keyword1 / keyword2 / remark.
        Users must have followed the account to receive messages.
        """
        cfg = self.cfg.get("wechat_mp", {})
        appid       = cfg.get("appid", "")
        appsecret   = cfg.get("appsecret", "")
        template_id = cfg.get("template_id", "")
        openids     = cfg.get("openids", [])

        if not appid or not appsecret or not template_id or not openids:
            logger.error(
                "wechat_mp: appid / appsecret / template_id / openids "
                "must all be configured"
            )
            return False

        try:
            token = self._wx_access_token(appid, appsecret)
            if not token:
                return False

            date_str = time.strftime("%Y-%m-%d %H:%M")
            # WeChat template fields have a ~200-char limit per value
            short_content = content[:500] + "…" if len(content) > 500 else content
            link = url or cfg.get("url", "")

            template_data = {
                "first":    {"value": title,         "color": "#173177"},
                "keyword1": {"value": date_str,       "color": "#173177"},
                "keyword2": {"value": short_content,  "color": "#333333"},
                "remark":   {"value": "点击查看完整信号报告", "color": "#999999"},
            }

            send_url = f"{_WX_SEND_URL}?access_token={token}"
            ok_count = 0
            for openid in openids:
                payload: Dict = {
                    "touser":      openid,
                    "template_id": template_id,
                    "data":        template_data,
                }
                if link:
                    payload["url"] = link
                r = requests.post(send_url, json=payload, timeout=_TIMEOUT)
                r.raise_for_status()
                resp = r.json()
                if resp.get("errcode", -1) == 0:
                    ok_count += 1
                else:
                    logger.warning(f"WeChat MP send to {openid} failed: {resp}")

            logger.info(f"WeChat MP: {ok_count}/{len(openids)} delivered")
            return ok_count == len(openids)

        except Exception as e:
            logger.error(f"WeChat MP failed: {e}")
            return False

    def _wx_access_token(self, appid: str, appsecret: str) -> Optional[str]:
        """Fetch WeChat access_token, cached in memory until 200 s before expiry."""
        now = time.time()
        cached = self._wx_token_cache.get(appid)
        if cached:
            token, expires_at = cached
            if now < expires_at:
                return token

        try:
            r = requests.get(
                _WX_TOKEN_URL,
                params={
                    "grant_type": "client_credential",
                    "appid":      appid,
                    "secret":     appsecret,
                },
                timeout=_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json()
            if "access_token" not in data:
                logger.error(f"WeChat MP token error: {data}")
                return None
            token      = data["access_token"]
            expires_in = data.get("expires_in", 7200)
            self._wx_token_cache[appid] = (token, now + expires_in - 200)
            return token
        except Exception as e:
            logger.error(f"WeChat MP token fetch failed: {e}")
            return None
