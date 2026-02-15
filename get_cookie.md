# Get Douyin Cookie

This guide explains how to get a valid Douyin `cookie` value and put it into `message_handlers.yml` as `auth_cookie`.

## Method 1 (Recommended): Browser DevTools

1. Open `https://live.douyin.com` in Chrome or Edge.
2. Log in with your account.
3. Press `F12` to open Developer Tools.
4. Go to the `Network` tab.
5. Refresh the page (`Ctrl + R`).
6. Click any request to `live.douyin.com`.
7. In `Request Headers`, find `cookie`.
8. Copy the full cookie string.
9. Open `message_handlers.yml` and set:

```yaml
auth_cookie: 'PASTE_FULL_COOKIE_HERE'
```

## Method 2: Application/Cookies panel

1. Open Developer Tools (`F12`).
2. Go to `Application` (or `Storage`) tab.
3. Open `Cookies` -> `https://live.douyin.com`.
4. Copy important cookie entries and combine them as `key=value; key2=value2; ...`.
5. Paste into `auth_cookie` in `message_handlers.yml`.

## Method 3 (Helper Script): get_cookie.py

1. In project root, run:

```bash
python get_cookie.py
```

2. Paste the full cookie string when prompted.
3. The script will:
	- validate basic cookie format
	- update `auth_cookie` in `message_handlers.yml`
	- create a timestamped backup of the original YAML file

This is the easiest way to avoid manual YAML editing mistakes.

## Verify Cookie Works

Run:

```bash
python main.py
```

If valid, the app should connect and start receiving live messages.

## Security Notes

- Keep cookie on one single line.
- Do not share cookie in screenshots, chat logs, or Git commits.
- Cookie may expire; refresh it if connection/messages fail.
- Prefer using a dedicated test account for scraping experiments.
