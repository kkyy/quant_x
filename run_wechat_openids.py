#!/usr/bin/env python3
"""List follower OpenIDs for the configured WeChat Official Account."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.utils.config import load_config

TOKEN_URL = "https://api.weixin.qq.com/cgi-bin/token"
USER_LIST_URL = "https://api.weixin.qq.com/cgi-bin/user/get"
USER_INFO_URL = "https://api.weixin.qq.com/cgi-bin/user/info"
TIMEOUT = 12


def _wechat_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    if config.get("notify", {}).get("wechat_mp"):
        return config["notify"]["wechat_mp"]
    return config.get("wechat_mp", {})


def _access_token(appid: str, appsecret: str) -> str:
    try:
        resp = requests.get(
            TOKEN_URL,
            params={
                "grant_type": "client_credential",
                "appid": appid,
                "secret": appsecret,
            },
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"WeChat token request failed: {exc.__class__.__name__}") from None
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"WeChat token error: {data}")
    return data["access_token"]


def _list_openids(token: str) -> List[str]:
    openids: List[str] = []
    next_openid = ""

    while True:
        params = {"access_token": token}
        if next_openid:
            params["next_openid"] = next_openid
        try:
            resp = requests.get(USER_LIST_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"WeChat user list request failed: {exc.__class__.__name__}") from None
        data = resp.json()
        if data.get("errcode"):
            raise RuntimeError(f"WeChat user list error: {data}")

        openids.extend(data.get("data", {}).get("openid", []) or [])
        next_openid = data.get("next_openid", "")
        if not next_openid or len(openids) >= int(data.get("total", 0)):
            return openids


def _user_info(token: str, openid: str) -> Dict[str, Any]:
    try:
        resp = requests.get(
            USER_INFO_URL,
            params={
                "access_token": token,
                "openid": openid,
                "lang": "zh_CN",
            },
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"WeChat user info request failed: {exc.__class__.__name__}") from None
    data = resp.json()
    if data.get("errcode"):
        raise RuntimeError(f"WeChat user info error for {openid}: {data}")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None, help="Optional config override YAML.")
    parser.add_argument("--details", action="store_true", help="Also fetch nickname/subscribe time.")
    args = parser.parse_args()

    config = load_config(args.config)
    wx_cfg = _wechat_cfg(config)
    appid = wx_cfg.get("appid", "")
    appsecret = wx_cfg.get("appsecret", "")
    if not appid or not appsecret:
        print("wechat_mp.appid and wechat_mp.appsecret are required in config/notify.yaml")
        return 1

    token = _access_token(appid, appsecret)
    openids = _list_openids(token)
    print(f"Found {len(openids)} follower OpenID(s):")

    if not args.details:
        for openid in openids:
            print(openid)
        return 0

    for openid in openids:
        info = _user_info(token, openid)
        nickname = info.get("nickname", "")
        subscribe = info.get("subscribe", 0)
        print(f"{openid}\tsubscribe={subscribe}\tnickname={nickname}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
