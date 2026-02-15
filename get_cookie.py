#!/usr/bin/env python
# coding: utf-8

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re


def _escape_single_quotes(value: str) -> str:
    return value.replace("'", "''")


def _is_reasonable_cookie(cookie: str) -> bool:
    if not cookie or "=" not in cookie:
        return False
    if len(cookie) < 20:
        return False
    return True


def update_auth_cookie_in_yaml(yaml_path: Path, cookie: str) -> bool:
    content = yaml_path.read_text(encoding="utf-8")

    pattern = re.compile(r"^(\s*auth_cookie\s*:\s*)(['\"])(.*?)(\2)\s*(#.*)?$", re.MULTILINE)

    escaped_cookie = _escape_single_quotes(cookie)

    def _replacer(match: re.Match) -> str:
        prefix = match.group(1)
        suffix_comment = match.group(5) or ""
        return f"{prefix}'{escaped_cookie}'{suffix_comment}"

    new_content, count = pattern.subn(_replacer, content, count=1)

    if count == 0:
        # Fallback: append auth_cookie if key does not exist.
        if not content.endswith("\n"):
            content += "\n"
        new_content = content + f"auth_cookie: '{escaped_cookie}'\n"

    if new_content == content:
        return False

    backup_name = f"{yaml_path.name}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_path = yaml_path.with_name(backup_name)
    backup_path.write_text(content, encoding="utf-8")

    yaml_path.write_text(new_content, encoding="utf-8")
    return True


def main() -> None:
    print("=== Douyin Cookie Helper ===")
    print("Paste full cookie string from browser Request Headers.")
    print("Then press Enter. (Single-line cookie expected)")

    cookie = input("\nCookie: ").strip()

    if not _is_reasonable_cookie(cookie):
        print("\n[Error] Cookie seems invalid. Please paste full cookie string.")
        return

    workspace_root = Path(__file__).resolve().parent
    yaml_path = workspace_root / "message_handlers.yml"

    if not yaml_path.exists():
        print(f"\n[Error] Not found: {yaml_path}")
        return

    changed = update_auth_cookie_in_yaml(yaml_path, cookie)

    if changed:
        print("\n[OK] auth_cookie updated in message_handlers.yml")
        print("[Info] A timestamped backup file was created in the same folder.")
        print("[Next] Run: python main.py")
    else:
        print("\n[Info] No changes made (same cookie may already be set).")


if __name__ == "__main__":
    main()
