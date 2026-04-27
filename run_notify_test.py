#!/usr/bin/env python3
"""Send a small test notification through one configured channel."""
from __future__ import annotations

import argparse
import copy
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.notify.pusher import NotificationPusher
from quant_ex.utils.config import load_config


CHANNELS = ("pushplus", "bark", "dingtalk", "serverchan", "wechat_mp")
REQUIRED_FIELDS = {
    "pushplus": ("token",),
    "bark": ("device_key",),
    "dingtalk": ("webhook_url",),
    "serverchan": ("send_key",),
    "wechat_mp": ("appid", "appsecret", "template_id", "openids"),
}


def _notify_root(config: Dict[str, Any]) -> Dict[str, Any]:
    if "notify" in config and isinstance(config["notify"], dict):
        return copy.deepcopy(config["notify"])
    return {
        key: copy.deepcopy(config.get(key, {}))
        for key in CHANNELS
        if key in config
    }


def _missing_fields(channel_cfg: Dict[str, Any], channel: str) -> list[str]:
    missing = []
    for field in REQUIRED_FIELDS[channel]:
        value = channel_cfg.get(field)
        if value in ("", None, []):
            missing.append(field)
        if field == "openids" and value == [""]:
            missing.append(field)
    return missing


def _config_for_channel(config: Dict[str, Any], channel: str) -> Dict[str, Any]:
    notify_cfg = _notify_root(config)
    if channel not in notify_cfg:
        notify_cfg[channel] = {}

    missing = _missing_fields(notify_cfg[channel], channel)
    if missing:
        raise ValueError(f"{channel} missing required field(s): {', '.join(sorted(set(missing)))}")

    for name in CHANNELS:
        notify_cfg.setdefault(name, {})
        notify_cfg[name]["enabled"] = name == channel
    patched = copy.deepcopy(config)
    patched["notify"] = notify_cfg
    return patched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None, help="Optional config override YAML.")
    parser.add_argument("--channel", choices=CHANNELS, required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--content", default="")
    parser.add_argument("--url", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    try:
        test_config = _config_for_channel(config, args.channel)
    except ValueError as exc:
        print(f"Config error: {exc}")
        return 1

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    title = args.title or f"quant_ex {args.channel} 通知测试"
    content = args.content or (
        f"这是一条来自 quant_ex 的测试通知。\n\n"
        f"渠道: {args.channel}\n"
        f"时间: {now}\n\n"
        "如果你收到这条消息，说明通知通道已配置成功。"
    )

    results = NotificationPusher(test_config).send(title, content, url=args.url)
    ok = results.get(args.channel, False)
    print(f"{args.channel}: {'ok' if ok else 'failed'}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
