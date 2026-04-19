
"""
GamerPower Telegram VIP Tracker - Final Edition
Developer / Owner tag: DevChaggy0x1
Powered by GamerPower API with attribution to GamerPower.com.
"""

from __future__ import annotations

import html
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

GAMERPOWER_API_URL = "https://www.gamerpower.com/api/giveaways?sort-by=date"
TELEGRAM_API_BASE = "https://api.telegram.org"
STATE_FILE = Path("state.json")
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
USER_AGENT = "GamerPowerTelegramVIPFinal/8.0 (Developer: DevChaggy0x1; Source: GamerPower.com)"
MAX_CAPTION = 1024


class BotError(Exception):
    pass


def utc_now_ts() -> int:
    return int(time.time())


def utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_dt(value: str) -> Optional[datetime]:
    if not value or value in ("N/A", "0000-00-00 00:00:00", "null", "None"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def human_remaining(end_date: str) -> str:
    dt = parse_dt(end_date)
    if not dt:
        return "غير معروف"
    diff = int(dt.timestamp()) - utc_now_ts()
    if diff <= 0:
        return "انتهى أو أوشك"
    days = diff // 86400
    hours = (diff % 86400) // 3600
    mins = (diff % 3600) // 60
    parts = []
    if days:
        parts.append(f"{days} يوم")
    if hours:
        parts.append(f"{hours} ساعة")
    if mins and days == 0:
        parts.append(f"{mins} دقيقة")
    return " و ".join(parts) if parts else "أقل من دقيقة"


def default_pref() -> Dict[str, Any]:
    return {
        "platform_whitelist": [],
        "type_whitelist": [],
        "only_active": True,
        "min_worth_usd": 0.0,
        "mute_expiring": False,
        "digest_only": False,
    }


def load_state() -> Dict[str, Any]:
    default = {
        "first_run_completed": False,
        "seen_ids": [],
        "telegram_offset": 0,
        "subscribers": {},
        "user_profiles": {},
        "claims": {},
        "clicks": {},
        "stats": {
            "notifications_sent": 0,
            "new_giveaways_detected": 0,
            "claim_reports": 0,
            "near_expiry_alerts_sent": 0,
            "digest_messages_sent": 0,
            "last_boot_ok": False,
        },
        "config": {
            "owner_id": os.getenv("OWNER_TELEGRAM_ID", "").strip(),
            "admin_ids": [],
            "notify_new": True,
            "notify_expiring": True,
            "expiring_window_hours": 12,
            "send_to_private_subscribers": True,
            "broadcast_chat_ids": [],
            "digest_enabled": True,
            "digest_max_items": 5,
            "vip_badge": "💎 VIP",
        },
        "expiring_alerted_ids": [],
        "digest_seen_ids": [],
        "runtime": {
            "bot_username": "",
            "bot_id": "",
            "last_error": "",
            "webhook_deleted": False,
            "best_today_last_sent_date": "",
        },
    }
    if not STATE_FILE.exists():
        return default
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return default
    if not isinstance(data, dict):
        return default
    for k, v in default.items():
        data.setdefault(k, v)
    for key in ("seen_ids", "expiring_alerted_ids", "digest_seen_ids"):
        data[key] = [str(x) for x in data.get(key, [])]
    for key in ("subscribers", "user_profiles", "claims", "clicks"):
        data[key] = {str(k): v for k, v in data.get(key, {}).items()}
    cfg = data["config"]
    cfg["owner_id"] = str(cfg.get("owner_id", "")).strip()
    cfg["admin_ids"] = [str(x) for x in cfg.get("admin_ids", [])]
    cfg["broadcast_chat_ids"] = [str(x) for x in cfg.get("broadcast_chat_ids", [])]
    return data


def save_state(state: Dict[str, Any]) -> None:
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def request_json(method: str, url: str, **kwargs) -> Any:
    headers = kwargs.pop("headers", {})
    headers["User-Agent"] = USER_AGENT
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs)
            if resp.status_code == 201 and "gamerpower.com/api" in url:
                return []
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(attempt * 2)
            else:
                break
    raise BotError(f"Request failed for {url}: {last_error}")


def telegram_api(token: str, method: str) -> str:
    return f"{TELEGRAM_API_BASE}/bot{token}/{method}"


def tg_request(token: str, method: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    data = request_json("POST", telegram_api(token, method), json=payload or {}, headers={"Content-Type": "application/json"})
    if not data.get("ok"):
        raise BotError(f"Telegram API error in {method}: {data}")
    return data["result"]


def validate_token_and_prepare_polling(state: Dict[str, Any], token: str) -> None:
    me = tg_request(token, "getMe", {})
    state["runtime"]["bot_username"] = me.get("username", "")
    state["runtime"]["bot_id"] = str(me.get("id", ""))
    tg_request(token, "deleteWebhook", {"drop_pending_updates": False})
    state["runtime"]["webhook_deleted"] = True
    tg_request(token, "setMyCommands", {
        "commands": [
            {"command": "start", "description": "تفعيل الاشتراك"},
            {"command": "stop", "description": "إيقاف الاشتراك"},
            {"command": "me", "description": "ملفك وإحصاءاتك"},
            {"command": "top", "description": "لوحة الشرف"},
            {"command": "panel", "description": "لوحتك"},
            {"command": "help", "description": "المساعدة"},
            {"command": "besttoday", "description": "أقوى عرض اليوم"},
            {"command": "owner", "description": "لوحة الإدارة"},
        ]
    })


def send_message(token: str, chat_id: str, text: str, reply_markup: Optional[Dict[str, Any]] = None, disable_preview: bool = True) -> None:
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": disable_preview}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_request(token, "sendMessage", payload)


def maybe_send_manual_run_notification(state: Dict[str, Any], token: str) -> None:
    if (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower() != "workflow_dispatch":
        return
    owner_id = str(state["config"].get("owner_id", "")).strip()
    if not owner_id:
        return
    send_message(
        token,
        owner_id,
        "🚀 <b>تم تشغيل الـ workflow يدويًا بنجاح</b>\n\n"
        "✅ البوت اشتغل\n"
        "✅ التوكن سليم\n"
        "✅ Telegram متصل\n"
        "💎 DevChaggy0x1 VIP Bot Ready"
    )


def fetch_giveaways() -> List[Dict[str, Any]]:
    data = request_json("GET", GAMERPOWER_API_URL, headers={"Accept": "application/json"})
    if not isinstance(data, list):
        raise BotError("Unexpected GamerPower API response")
    return data


def normalize_giveaway(item: Dict[str, Any]) -> Dict[str, str]:
    gid = str(item.get("id", "")).strip()
    if not gid:
        raise BotError("Missing giveaway id")
    return {
        "id": gid,
        "title": str(item.get("title") or "No title"),
        "worth": str(item.get("worth") or "N/A"),
        "platforms": str(item.get("platforms") or "Unknown"),
        "type": str(item.get("type") or "Unknown"),
        "users": str(item.get("users") or "N/A"),
        "end_date": str(item.get("end_date") or "N/A"),
        "published_date": str(item.get("published_date") or "N/A"),
        "status": str(item.get("status") or "Active"),
        "open_giveaway_url": str(item.get("open_giveaway_url") or item.get("gamerpower_url") or ""),
        "gamerpower_url": str(item.get("gamerpower_url") or ""),
        "thumbnail": str(item.get("thumbnail") or ""),
        "description": str(item.get("description") or "No description."),
        "instructions": str(item.get("instructions") or "No instructions provided."),
    }


def escape(text: str) -> str:
    return html.escape(text or "", quote=False)


def ensure_profile(state: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
    uid = str(user["id"])
    if uid not in state["user_profiles"]:
        state["user_profiles"][uid] = {
            "first_name": user.get("first_name", ""),
            "last_name": user.get("last_name", ""),
            "username": user.get("username", ""),
            "display_name": user.get("first_name", ""),
            "claims_count": 0,
            "clicked_count": 0,
            "registered_at": utc_now_ts(),
            "prefs": default_pref(),
        }
    else:
        prof = state["user_profiles"][uid]
        prof["first_name"] = user.get("first_name", prof.get("first_name", ""))
        prof["last_name"] = user.get("last_name", prof.get("last_name", ""))
        prof["username"] = user.get("username", prof.get("username", ""))
        prof.setdefault("prefs", default_pref())
    return state["user_profiles"][uid]


def user_label(profile: Dict[str, Any], user_id: str) -> str:
    if profile.get("display_name"):
        return profile["display_name"]
    if profile.get("username"):
        return "@" + profile["username"]
    if profile.get("first_name"):
        return profile["first_name"]
    return f"User {user_id}"


def is_owner(state: Dict[str, Any], user_id: str) -> bool:
    return user_id and user_id == str(state["config"].get("owner_id", "")).strip()


def is_admin_or_owner(state: Dict[str, Any], user_id: str) -> bool:
    return is_owner(state, user_id) or user_id in set(state["config"].get("admin_ids", []))


def worth_to_float(worth: str) -> float:
    if not worth or worth == "N/A":
        return 0.0
    cleaned = worth.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except Exception:
        return 0.0


def item_matches_prefs(item: Dict[str, str], prefs: Dict[str, Any]) -> bool:
    if prefs.get("only_active", True) and item.get("status", "").lower() != "active":
        return False
    if worth_to_float(item.get("worth", "0")) < float(prefs.get("min_worth_usd", 0.0)):
        return False
    p_whitelist = [x.lower() for x in prefs.get("platform_whitelist", [])]
    if p_whitelist:
        platforms = item.get("platforms", "").lower()
        if not any(p in platforms for p in p_whitelist):
            return False
    t_whitelist = [x.lower() for x in prefs.get("type_whitelist", [])]
    if t_whitelist:
        t = item.get("type", "").lower()
        if not any(x in t for x in t_whitelist):
            return False
    return True


def build_main_buttons(item: Dict[str, str]) -> Dict[str, Any]:
    gid = item["id"]
    url = item["open_giveaway_url"] or item["gamerpower_url"] or "https://www.gamerpower.com/"
    details_url = item["gamerpower_url"] or url
    return {
        "inline_keyboard": [
            [{"text": "🎁 افتح العرض مباشرة", "url": url}],
            [
                {"text": "✅ سجلت أني أخذتها", "callback_data": f"claim:{gid}"},
                {"text": "📄 التفاصيل", "url": details_url},
            ],
            [
                {"text": "⚙️ تفضيلاتي", "callback_data": "panel:prefs"},
                {"text": "🏆 الترتيب", "callback_data": "panel:top"},
            ],
        ]
    }


def build_panel_markup() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "👤 ملفي", "callback_data": "panel:me"}, {"text": "🏆 الترتيب", "callback_data": "panel:top"}],
            [{"text": "⚙️ تفضيلاتي", "callback_data": "panel:prefs"}, {"text": "🌟 أقوى عرض اليوم", "callback_data": "panel:besttoday"}],
        ]
    }


def build_owner_panel(state: Dict[str, Any]) -> Dict[str, Any]:
    cfg = state["config"]
    return {
        "inline_keyboard": [
            [
                {"text": f"الجديد {'✅' if cfg.get('notify_new', True) else '❌'}", "callback_data": "owner:toggle_notify_new"},
                {"text": f"القرب {'✅' if cfg.get('notify_expiring', True) else '❌'}", "callback_data": "owner:toggle_notify_expiring"},
            ],
            [
                {"text": f"Digest {'✅' if cfg.get('digest_enabled', True) else '❌'}", "callback_data": "owner:toggle_digest"},
                {"text": f"الخاص {'✅' if cfg.get('send_to_private_subscribers', True) else '❌'}", "callback_data": "owner:toggle_private"},
            ],
            [
                {"text": "📊 الإحصاءات", "callback_data": "owner:stats"},
                {"text": "👥 المشتركون", "callback_data": "owner:subs"},
            ],
        ]
    }


def format_game_card(item: Dict[str, str], vip_badge: str, expiring: bool = False) -> str:
    desc = item["description"].strip()
    if len(desc) > 420:
        desc = desc[:417] + "..."
    instr = item["instructions"].strip()
    if len(instr) > 280:
        instr = instr[:277] + "..."
    remaining = human_remaining(item["end_date"])
    badge = "🔥 <b>فرصة أخيرة</b>\n\n" if expiring else f"{vip_badge} <b>كشف جديد</b>\n\n"
    return (
        f"{badge}"
        f"🎮 <b>{escape(item['title'])}</b>\n"
        f"💻 <b>المنصات:</b> {escape(item['platforms'])}\n"
        f"🕹 <b>النوع:</b> {escape(item['type'])}\n"
        f"💰 <b>القيمة:</b> {escape(item['worth'])}\n"
        f"📅 <b>نزلت مجانًا:</b> {escape(item['published_date'])}\n"
        f"⏳ <b>تنتهي:</b> {escape(item['end_date'])}\n"
        f"⌛ <b>المتبقي:</b> {escape(remaining)}\n"
        f"📈 <b>الحالة:</b> {escape(item['status'])}\n"
        f"👥 <b>المهتمون:</b> {escape(item['users'])}\n\n"
        f"📝 <b>الوصف:</b> {escape(desc)}\n\n"
        f"📌 <b>طريقة الحصول:</b> {escape(instr)}\n\n"
        f"🌐 <b>المصدر:</b> GamerPower.com\n"
        f"👨‍💻 <b>Developer:</b> DevChaggy0x1"
    )


def send_game_message(token: str, chat_id: str, item: Dict[str, str], vip_badge: str, expiring: bool = False) -> None:
    text = format_game_card(item, vip_badge=vip_badge, expiring=expiring)
    reply_markup = build_main_buttons(item)
    if item["thumbnail"]:
        try:
            tg_request(token, "sendPhoto", {
                "chat_id": chat_id,
                "photo": item["thumbnail"],
                "caption": text[:MAX_CAPTION],
                "parse_mode": "HTML",
                "reply_markup": reply_markup,
                "disable_web_page_preview": False,
            })
            return
        except Exception:
            pass
    send_message(token, chat_id, text, reply_markup=reply_markup, disable_preview=False)


def edit_message(token: str, chat_id: str, message_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_request(token, "editMessageText", payload)


def answer_callback(token: str, callback_query_id: str, text: str, show_alert: bool = False) -> None:
    tg_request(token, "answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text[:200], "show_alert": show_alert})


def get_updates(token: str, offset: int) -> List[Dict[str, Any]]:
    return tg_request(token, "getUpdates", {"offset": offset, "timeout": 0, "allowed_updates": ["message", "callback_query"]})


def subscribe_private(state: Dict[str, Any], user: Dict[str, Any], chat_id: str) -> None:
    state["subscribers"][chat_id] = {"active": True, "user_id": str(user["id"]), "added_at": utc_now_ts(), "mode": "private"}


def prefs_text(profile: Dict[str, Any], user_id: str) -> str:
    prefs = profile.get("prefs", default_pref())
    p_list = ", ".join(prefs.get("platform_whitelist", [])) or "الكل"
    t_list = ", ".join(prefs.get("type_whitelist", [])) or "الكل"
    return (
        f"⚙️ <b>تفضيلات {escape(user_label(profile, user_id))}</b>\n\n"
        f"المنصات: <b>{escape(p_list)}</b>\n"
        f"الأنواع: <b>{escape(t_list)}</b>\n"
        f"أقل قيمة: <b>{prefs.get('min_worth_usd', 0)}</b>$\n"
        f"كتم تنبيهات القرب: <b>{'نعم' if prefs.get('mute_expiring', False) else 'لا'}</b>\n"
        f"Digest only: <b>{'نعم' if prefs.get('digest_only', False) else 'لا'}</b>\n\n"
        f"<b>أوامر التخصيص:</b>\n"
        f"/platform steam,epic games\n"
        f"/type game,loot,beta\n"
        f"/minworth 10\n"
        f"/digestonly on|off\n"
        f"/muteexpiring on|off\n"
        f"/resetprefs"
    )


def build_top_text(state: Dict[str, Any]) -> str:
    ranking = []
    for uid, prof in state["user_profiles"].items():
        ranking.append((len(state["claims"].get(uid, [])), user_label(prof, uid)))
    ranking.sort(key=lambda x: (-x[0], x[1].lower()))
    if not ranking:
        return "لا توجد إحصاءات بعد."
    lines = ["🏆 <b>لوحة الشرف VIP</b>\n"]
    for i, (count, name) in enumerate(ranking[:15], start=1):
        lines.append(f"{i}. <b>{escape(name)}</b> — {count} عرض")
    return "\n".join(lines)


def pick_best_today(items: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    today = utc_today_str()
    todays = [x for x in items if str(x.get("published_date", "")).startswith(today)]
    pool = todays if todays else items
    active_pool = [x for x in pool if x.get("status", "").lower() == "active"] or pool
    if not active_pool:
        return None
    return max(active_pool, key=lambda x: (worth_to_float(x.get("worth", "0")), int(x.get("users", "0")) if str(x.get("users", "0")).isdigit() else 0))


def best_today_text(item: Dict[str, str]) -> str:
    return (
        "🌟 <b>أقوى عرض اليوم</b>\n\n"
        f"🎮 <b>{escape(item['title'])}</b>\n"
        f"💰 القيمة: <b>{escape(item['worth'])}</b>\n"
        f"💻 المنصات: <b>{escape(item['platforms'])}</b>\n"
        f"🕹 النوع: <b>{escape(item['type'])}</b>\n"
        f"📅 نزلت: <b>{escape(item['published_date'])}</b>\n"
        f"⏳ تنتهي: <b>{escape(item['end_date'])}</b>\n"
        f"👥 المهتمون: <b>{escape(item['users'])}</b>\n"
    )


def owner_stats_text(state: Dict[str, Any]) -> str:
    cfg = state["config"]
    runtime = state["runtime"]
    subs_total = len(state["subscribers"])
    subs_active = sum(1 for x in state["subscribers"].values() if x.get("active"))
    return (
        f"{escape(cfg.get('vip_badge', '💎 VIP'))} <b>لوحة المالك</b>\n\n"
        f"اسم البوت: <b>@{escape(runtime.get('bot_username', ''))}</b>\n"
        f"BOT ID: <b>{escape(runtime.get('bot_id', ''))}</b>\n"
        f"Webhook deleted: <b>{'yes' if runtime.get('webhook_deleted') else 'no'}</b>\n"
        f"المشتركون: <b>{subs_active}/{subs_total}</b>\n"
        f"الإشعارات المرسلة: <b>{state['stats'].get('notifications_sent', 0)}</b>\n"
        f"العروض الجديدة: <b>{state['stats'].get('new_giveaways_detected', 0)}</b>\n"
        f"آخر خطأ: <b>{escape(runtime.get('last_error', '') or 'لا يوجد')}</b>"
    )


def on_off_value(arg: str) -> Optional[bool]:
    arg = arg.strip().lower()
    if arg in ("on", "yes", "1", "true"):
        return True
    if arg in ("off", "no", "0", "false"):
        return False
    return None


def process_start(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    chat = message["chat"]
    if chat.get("type") != "private":
        return
    user = message["from"]
    chat_id = str(chat["id"])
    profile = ensure_profile(state, user)
    subscribe_private(state, user, chat_id)
    owner_badge = " 👑" if is_owner(state, str(user["id"])) else ""
    text = (
        f"{escape(state['config'].get('vip_badge', '💎 VIP'))} أهلاً <b>{escape(user_label(profile, str(user['id'])))}</b>{owner_badge}\n\n"
        "تم تفعيل اشتراكك بنجاح ✅\n"
        "هذا البوت يعمل عبر GitHub Actions، لذلك يقرأ الأوامر أثناء تشغيل الـ workflow.\n"
    )
    send_message(token, chat_id, text, reply_markup=build_panel_markup())


def process_stop(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    chat_id = str(message["chat"]["id"])
    if chat_id in state["subscribers"]:
        state["subscribers"][chat_id]["active"] = False
    send_message(token, chat_id, "تم إيقاف الإشعارات. أرسل /start لإعادة التفعيل.")


def process_help(token: str, message: Dict[str, Any]) -> None:
    send_message(token, str(message["chat"]["id"]), "استخدم /start ثم شغّل Run workflow للاختبار الفوري.")


def process_me(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    uid = str(user["id"])
    text = (
        f"👤 <b>ملفك VIP</b>\n\n"
        f"الاسم: <b>{escape(user_label(profile, uid))}</b>\n"
        f"المعرف: <b>{escape('@' + profile['username'] if profile.get('username') else 'غير موجود')}</b>\n"
        f"عدد العروض المسجلة: <b>{len(state['claims'].get(uid, []))}</b>\n\n"
        f"{prefs_text(profile, uid)}"
    )
    send_message(token, chat_id, text)


def process_top(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    send_message(token, str(message["chat"]["id"]), build_top_text(state))


def process_panel(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    send_message(token, str(message["chat"]["id"]), "🎛 <b>لوحتك VIP</b>", reply_markup=build_panel_markup())


def process_owner(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    uid = str(message["from"]["id"])
    chat_id = str(message["chat"]["id"])
    if not is_admin_or_owner(state, uid):
        send_message(token, chat_id, "هذه اللوحة للمالك أو الأدمن فقط.")
        return
    send_message(token, chat_id, owner_stats_text(state), reply_markup=build_owner_panel(state))


def process_besttoday(state: Dict[str, Any], token: str, message: Dict[str, Any], items: List[Dict[str, str]]) -> None:
    item = pick_best_today(items)
    if not item:
        send_message(token, str(message["chat"]["id"]), "لا يوجد عرض مناسب حاليًا.")
        return
    send_message(token, str(message["chat"]["id"]), best_today_text(item), reply_markup=build_main_buttons(item), disable_preview=False)


def process_setname(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    profile = ensure_profile(state, message["from"])
    new_name = text[len("/setname"):].strip()
    if not new_name:
        send_message(token, str(message["chat"]["id"]), "اكتب: /setname اسمك")
        return
    profile["display_name"] = new_name[:50]
    send_message(token, str(message["chat"]["id"]), f"تم حفظ الاسم: <b>{escape(profile['display_name'])}</b>")


def process_platform(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    profile = ensure_profile(state, message["from"])
    arg = text[len("/platform"):].strip()
    profile["prefs"]["platform_whitelist"] = [x.strip() for x in arg.split(",") if x.strip()] if arg else []
    send_message(token, str(message["chat"]["id"]), "تم تحديث فلتر المنصات.")


def process_type(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    profile = ensure_profile(state, message["from"])
    arg = text[len("/type"):].strip()
    profile["prefs"]["type_whitelist"] = [x.strip() for x in arg.split(",") if x.strip()] if arg else []
    send_message(token, str(message["chat"]["id"]), "تم تحديث فلتر الأنواع.")


def process_minworth(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    profile = ensure_profile(state, message["from"])
    arg = text[len("/minworth"):].strip()
    try:
        value = float(arg)
    except Exception:
        send_message(token, str(message["chat"]["id"]), "اكتب رقمًا صحيحًا مثل /minworth 10")
        return
    profile["prefs"]["min_worth_usd"] = max(value, 0.0)
    send_message(token, str(message["chat"]["id"]), f"تم حفظ الحد الأدنى: <b>{value}</b>$")


def process_digestonly(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    profile = ensure_profile(state, message["from"])
    val = on_off_value(text[len("/digestonly"):].strip())
    if val is None:
        send_message(token, str(message["chat"]["id"]), "استخدم on أو off")
        return
    profile["prefs"]["digest_only"] = val
    send_message(token, str(message["chat"]["id"]), f"Digest only: <b>{'ON' if val else 'OFF'}</b>")


def process_muteexpiring(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    profile = ensure_profile(state, message["from"])
    val = on_off_value(text[len("/muteexpiring"):].strip())
    if val is None:
        send_message(token, str(message["chat"]["id"]), "استخدم on أو off")
        return
    profile["prefs"]["mute_expiring"] = val
    send_message(token, str(message["chat"]["id"]), f"mute expiring: <b>{'ON' if val else 'OFF'}</b>")


def process_resetprefs(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    profile = ensure_profile(state, message["from"])
    profile["prefs"] = default_pref()
    send_message(token, str(message["chat"]["id"]), "تمت إعادة التفضيلات للوضع الافتراضي.")


def process_callback_claim(state: Dict[str, Any], token: str, callback_query: Dict[str, Any], giveaway_id: str) -> None:
    cqid = callback_query["id"]
    user = callback_query["from"]
    profile = ensure_profile(state, user)
    uid = str(user["id"])
    chat_id = str(callback_query.get("message", {}).get("chat", {}).get("id", uid))
    state["clicks"].setdefault(uid, [])
    state["clicks"][uid].append({"giveaway_id": giveaway_id, "ts": utc_now_ts()})
    profile["clicked_count"] = profile.get("clicked_count", 0) + 1
    claims = state["claims"].setdefault(uid, [])
    if giveaway_id not in claims:
        claims.append(giveaway_id)
        profile["claims_count"] = len(claims)
        state["stats"]["claim_reports"] += 1
        answer_callback(token, cqid, "تم التسجيل ✅")
        send_message(token, chat_id, f"🎉 تم تسجيل هذا العرض لك. الإجمالي: <b>{len(claims)}</b>")
    else:
        answer_callback(token, cqid, "هذا العرض مسجل عندك بالفعل.")


def handle_panel_callback(state: Dict[str, Any], token: str, callback_query: Dict[str, Any], action: str, items: List[Dict[str, str]]) -> None:
    cqid = callback_query["id"]
    msg = callback_query.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id"))
    message_id = msg.get("message_id")
    user = callback_query["from"]
    profile = ensure_profile(state, user)
    uid = str(user["id"])
    if action == "me":
        edit_message(token, chat_id, message_id, f"👤 <b>ملفك VIP</b>\n\nالاسم: <b>{escape(user_label(profile, uid))}</b>\n\n{prefs_text(profile, uid)}", reply_markup=build_panel_markup())
        answer_callback(token, cqid, "تم فتح ملفك.")
    elif action == "top":
        edit_message(token, chat_id, message_id, build_top_text(state), reply_markup=build_panel_markup())
        answer_callback(token, cqid, "تم فتح الترتيب.")
    elif action == "prefs":
        edit_message(token, chat_id, message_id, prefs_text(profile, uid), reply_markup=build_panel_markup())
        answer_callback(token, cqid, "تم فتح التفضيلات.")
    elif action == "besttoday":
        item = pick_best_today(items)
        if item:
            edit_message(token, chat_id, message_id, best_today_text(item), reply_markup=build_main_buttons(item))
        answer_callback(token, cqid, "تم فتح أقوى عرض اليوم.")


def handle_owner_callback(state: Dict[str, Any], token: str, callback_query: Dict[str, Any], action: str) -> None:
    user_id = str(callback_query["from"]["id"])
    cqid = callback_query["id"]
    msg = callback_query.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id"))
    message_id = msg.get("message_id")
    if not is_admin_or_owner(state, user_id):
        answer_callback(token, cqid, "غير مصرح.", show_alert=True)
        return
    cfg = state["config"]
    if action == "toggle_notify_new":
        cfg["notify_new"] = not cfg.get("notify_new", True)
        answer_callback(token, cqid, f"الجديد {'ON' if cfg['notify_new'] else 'OFF'}")
    elif action == "toggle_notify_expiring":
        cfg["notify_expiring"] = not cfg.get("notify_expiring", True)
        answer_callback(token, cqid, f"القرب {'ON' if cfg['notify_expiring'] else 'OFF'}")
    elif action == "toggle_digest":
        cfg["digest_enabled"] = not cfg.get("digest_enabled", True)
        answer_callback(token, cqid, f"Digest {'ON' if cfg['digest_enabled'] else 'OFF'}")
    elif action == "toggle_private":
        cfg["send_to_private_subscribers"] = not cfg.get("send_to_private_subscribers", True)
        answer_callback(token, cqid, f"الخاص {'ON' if cfg['send_to_private_subscribers'] else 'OFF'}")
    elif action == "stats":
        answer_callback(token, cqid, "تم تحديث الإحصاءات.")
    elif action == "subs":
        answer_callback(token, cqid, f"المشتركون: {len(state['subscribers'])}", show_alert=True)
    edit_message(token, chat_id, message_id, owner_stats_text(state), reply_markup=build_owner_panel(state))


def process_message_update(state: Dict[str, Any], token: str, message: Dict[str, Any], items: List[Dict[str, str]]) -> None:
    text = (message.get("text") or "").strip()
    if not text:
        return
    if message.get("from"):
        ensure_profile(state, message["from"])
    if text.startswith("/start"):
        process_start(state, token, message)
    elif text.startswith("/stop"):
        process_stop(state, token, message)
    elif text.startswith("/help"):
        process_help(token, message)
    elif text.startswith("/me"):
        process_me(state, token, message)
    elif text.startswith("/top"):
        process_top(state, token, message)
    elif text.startswith("/panel"):
        process_panel(state, token, message)
    elif text.startswith("/owner"):
        process_owner(state, token, message)
    elif text.startswith("/besttoday"):
        process_besttoday(state, token, message, items)
    elif text.startswith("/setname"):
        process_setname(state, token, message, text)
    elif text.startswith("/platform"):
        process_platform(state, token, message, text)
    elif text.startswith("/type"):
        process_type(state, token, message, text)
    elif text.startswith("/minworth"):
        process_minworth(state, token, message, text)
    elif text.startswith("/digestonly"):
        process_digestonly(state, token, message, text)
    elif text.startswith("/muteexpiring"):
        process_muteexpiring(state, token, message, text)
    elif text.startswith("/resetprefs"):
        process_resetprefs(state, token, message)


def process_updates(state: Dict[str, Any], token: str, items: List[Dict[str, str]]) -> None:
    updates = get_updates(token, int(state.get("telegram_offset", 0)))
    print(f"Telegram updates fetched: {len(updates)}")
    for upd in updates:
        state["telegram_offset"] = int(upd["update_id"]) + 1
        if "message" in upd:
            process_message_update(state, token, upd["message"], items)
        elif "callback_query" in upd:
            data = upd["callback_query"].get("data", "")
            if data.startswith("claim:"):
                process_callback_claim(state, token, upd["callback_query"], data.split(":", 1)[1].strip())
            elif data.startswith("panel:"):
                handle_panel_callback(state, token, upd["callback_query"], data.split(":", 1)[1].strip(), items)
            elif data.startswith("owner:"):
                handle_owner_callback(state, token, upd["callback_query"], data.split(":", 1)[1].strip())


def active_target_chat_ids(state: Dict[str, Any]) -> List[str]:
    cfg = state["config"]
    chats = []
    if cfg.get("send_to_private_subscribers", True):
        chats.extend([cid for cid, info in state["subscribers"].items() if info.get("active")])
    chats.extend(cfg.get("broadcast_chat_ids", []))
    out, seen = [], set()
    for c in chats:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def user_target_should_receive(state: Dict[str, Any], chat_id: str, item: Dict[str, str], expiring: bool = False) -> bool:
    sub = state["subscribers"].get(chat_id)
    if not sub:
        return True
    uid = str(sub.get("user_id", ""))
    profile = state["user_profiles"].get(uid)
    if not profile:
        return True
    prefs = profile.get("prefs", default_pref())
    if expiring and prefs.get("mute_expiring", False):
        return False
    if prefs.get("digest_only", False) and not expiring:
        return False
    return item_matches_prefs(item, prefs)


def giveaway_is_expiring(item: Dict[str, str], window_hours: int) -> bool:
    dt = parse_dt(item["end_date"])
    if not dt:
        return False
    diff = int(dt.timestamp()) - utc_now_ts()
    return 0 < diff <= int(window_hours) * 3600


def send_new_giveaways(state: Dict[str, Any], token: str, items: List[Dict[str, str]]) -> int:
    cfg = state["config"]
    if not cfg.get("notify_new", True):
        return 0
    current_ids = {x["id"] for x in items}
    seen_ids = set(state.get("seen_ids", []))
    if not state.get("first_run_completed", False):
        state["seen_ids"] = sorted(current_ids, key=lambda x: int(x))
        state["digest_seen_ids"] = sorted(current_ids, key=lambda x: int(x))
        state["first_run_completed"] = True
        print("First run: state initialized, old giveaways not sent.")
        return 0
    new_items = [x for x in items if x["id"] not in seen_ids]
    targets = active_target_chat_ids(state)
    print(f"New giveaways found: {len(new_items)} | active targets: {len(targets)}")
    for item in reversed(new_items):
        for chat_id in targets:
            if not user_target_should_receive(state, chat_id, item, expiring=False):
                continue
            try:
                send_game_message(token, chat_id, item, vip_badge=cfg.get("vip_badge", "💎 VIP"), expiring=False)
                state["stats"]["notifications_sent"] += 1
                time.sleep(0.15)
            except Exception as exc:
                print(f"Failed sending new giveaway {item['id']} to {chat_id}: {exc}")
    state["stats"]["new_giveaways_detected"] += len(new_items)
    state["seen_ids"] = sorted(current_ids.union(seen_ids), key=lambda x: int(x))
    return len(new_items)


def send_expiring_alerts(state: Dict[str, Any], token: str, items: List[Dict[str, str]]) -> int:
    cfg = state["config"]
    if not cfg.get("notify_expiring", True):
        return 0
    targets = active_target_chat_ids(state)
    already = set(state.get("expiring_alerted_ids", []))
    count = 0
    for item in items:
        if item["id"] in already:
            continue
        if giveaway_is_expiring(item, int(cfg.get("expiring_window_hours", 12))):
            for chat_id in targets:
                if not user_target_should_receive(state, chat_id, item, expiring=True):
                    continue
                try:
                    send_game_message(token, chat_id, item, vip_badge=cfg.get("vip_badge", "💎 VIP"), expiring=True)
                    state["stats"]["near_expiry_alerts_sent"] += 1
                    time.sleep(0.15)
                except Exception as exc:
                    print(f"Failed sending expiring giveaway {item['id']} to {chat_id}: {exc}")
            already.add(item["id"])
            count += 1
    state["expiring_alerted_ids"] = sorted(already, key=lambda x: int(x))
    return count


def build_digest_text(items: List[Dict[str, str]], vip_badge: str) -> str:
    lines = [f"{escape(vip_badge)} <b>VIP Digest</b>\n"]
    for item in items:
        url = item["open_giveaway_url"] or item["gamerpower_url"] or "https://www.gamerpower.com/"
        lines.append(
            f"• <b>{escape(item['title'])}</b>\n"
            f"  {escape(item['type'])} | {escape(item['platforms'])}\n"
            f"  القيمة: {escape(item['worth'])} | ينتهي: {escape(item['end_date'])}\n"
            f"  <a href=\"{html.escape(url, quote=True)}\">افتح العرض</a>\n"
        )
    return "\n".join(lines)


def send_digest(state: Dict[str, Any], token: str, items: List[Dict[str, str]]) -> int:
    cfg = state["config"]
    if not cfg.get("digest_enabled", True):
        return 0
    seen = set(state.get("digest_seen_ids", []))
    fresh = [x for x in items if x["id"] not in seen]
    if not fresh:
        return 0
    sent = 0
    max_items = int(cfg.get("digest_max_items", 5))
    for chat_id, sub in state["subscribers"].items():
        if not sub.get("active"):
            continue
        uid = str(sub.get("user_id", ""))
        profile = state["user_profiles"].get(uid)
        if not profile:
            continue
        prefs = profile.get("prefs", default_pref())
        if not prefs.get("digest_only", False):
            continue
        filtered = [x for x in fresh if item_matches_prefs(x, prefs)]
        if not filtered:
            continue
        chunk = filtered[:max_items]
        try:
            send_message(token, chat_id, build_digest_text(chunk, cfg.get("vip_badge", "💎 VIP")), disable_preview=False)
            state["stats"]["digest_messages_sent"] += 1
            sent += 1
            time.sleep(0.15)
        except Exception as exc:
            print(f"Failed sending digest to {chat_id}: {exc}")
    state["digest_seen_ids"] = sorted(set(state.get("digest_seen_ids", [])).union({x["id"] for x in fresh}), key=lambda x: int(x))
    return sent


def maybe_send_best_today_to_owner(state: Dict[str, Any], token: str, items: List[Dict[str, str]]) -> None:
    owner_id = str(state["config"].get("owner_id", "")).strip()
    if not owner_id:
        return
    today = utc_today_str()
    if state["runtime"].get("best_today_last_sent_date") == today:
        return
    item = pick_best_today(items)
    if not item:
        return
    send_message(token, owner_id, best_today_text(item), reply_markup=build_main_buttons(item), disable_preview=False)
    state["runtime"]["best_today_last_sent_date"] = today


def validate_env() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise BotError("TELEGRAM_BOT_TOKEN is missing")
    return token


def main() -> int:
    state = load_state()
    token = validate_env()
    try:
        validate_token_and_prepare_polling(state, token)
        maybe_send_manual_run_notification(state, token)
        items = [normalize_giveaway(x) for x in fetch_giveaways()]
        maybe_send_best_today_to_owner(state, token, items)
        process_updates(state, token, items)
        new_count = send_new_giveaways(state, token, items)
        exp_count = send_expiring_alerts(state, token, items)
        digest_count = send_digest(state, token, items)
        state["stats"]["last_boot_ok"] = True
        state["runtime"]["last_error"] = ""
        save_state(state)
        print(f"Bot ready: @{state['runtime']['bot_username']} ({state['runtime']['bot_id']})")
        print(f"Done. new={new_count}, expiring={exp_count}, digest={digest_count}")
        return 0
    except Exception as exc:
        state["stats"]["last_boot_ok"] = False
        state["runtime"]["last_error"] = str(exc)
        save_state(state)
        print(f"FATAL: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
