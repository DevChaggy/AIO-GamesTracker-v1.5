"""
GamerPower Telegram VIP Tracker
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
from typing import Any, Dict, List, Optional, Set

import requests

GAMERPOWER_API_URL = "https://www.gamerpower.com/api/giveaways?sort-by=date"
TELEGRAM_API_BASE = "https://api.telegram.org"
STATE_FILE = Path("state.json")
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
USER_AGENT = "GamerPowerTelegramVIP/5.0 (Developer: DevChaggy0x1; Source: GamerPower.com)"
MAX_CAPTION = 1024


class BotError(Exception):
    pass


def utc_now_ts() -> int:
    return int(time.time())


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
            "url_button_events_trackable": False,
            "digest_messages_sent": 0,
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
            "theme": "luxury",
        },
        "expiring_alerted_ids": [],
        "digest_seen_ids": [],
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
    data["seen_ids"] = [str(x) for x in data.get("seen_ids", [])]
    data["expiring_alerted_ids"] = [str(x) for x in data.get("expiring_alerted_ids", [])]
    data["digest_seen_ids"] = [str(x) for x in data.get("digest_seen_ids", [])]
    data["subscribers"] = {str(k): v for k, v in data.get("subscribers", {}).items()}
    data["user_profiles"] = {str(k): v for k, v in data.get("user_profiles", {}).items()}
    data["claims"] = {str(k): v for k, v in data.get("claims", {}).items()}
    data["clicks"] = {str(k): v for k, v in data.get("clicks", {}).items()}
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


def telegram_api(token: str, method: str) -> str:
    return f"{TELEGRAM_API_BASE}/bot{token}/{method}"


def tg_request(token: str, method: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    url = telegram_api(token, method)
    data = request_json("POST", url, json=payload or {}, headers={"Content-Type": "application/json"})
    if not data.get("ok"):
        raise BotError(f"Telegram API error in {method}: {data}")
    return data["result"]


def escape(text: str) -> str:
    return html.escape(text or "", quote=False)


def default_pref() -> Dict[str, Any]:
    return {
        "platform_whitelist": [],
        "type_whitelist": [],
        "only_active": True,
        "min_worth_usd": 0.0,
        "mute_expiring": False,
        "digest_only": False,
    }


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
        f"╭──────────────╮\n"
        f"│ 🕹 <b>النوع:</b> {escape(item['type'])}\n"
        f"│ 💻 <b>المنصات:</b> {escape(item['platforms'])}\n"
        f"│ 💰 <b>القيمة:</b> {escape(item['worth'])}\n"
        f"│ 📅 <b>نزلت مجانًا:</b> {escape(item['published_date'])}\n"
        f"│ ⏳ <b>تنتهي:</b> {escape(item['end_date'])}\n"
        f"│ ⌛ <b>المتبقي:</b> {escape(remaining)}\n"
        f"│ 📈 <b>الحالة:</b> {escape(item['status'])}\n"
        f"│ 👥 <b>المهتمون:</b> {escape(item['users'])}\n"
        f"╰──────────────╯\n\n"
        f"📝 <b>الوصف:</b> {escape(desc)}\n\n"
        f"📌 <b>طريقة الحصول:</b> {escape(instr)}\n\n"
        f"🌐 <b>المصدر:</b> GamerPower.com\n"
        f"👨‍💻 <b>Developer:</b> DevChaggy0x1"
    )


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
            [
                {"text": "👤 ملفي", "callback_data": "panel:me"},
                {"text": "🏆 الترتيب", "callback_data": "panel:top"},
            ],
            [
                {"text": "⚙️ تفضيلاتي", "callback_data": "panel:prefs"},
                {"text": "📘 المساعدة", "callback_data": "panel:help"},
            ],
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
            [
                {"text": "➕ أضف الشات للبث", "callback_data": "owner:add_chat"},
                {"text": "➖ احذف الشات", "callback_data": "owner:remove_chat"},
            ],
            [
                {"text": "⏰ +1 ساعة", "callback_data": "owner:exp_plus"},
                {"text": "⏰ -1 ساعة", "callback_data": "owner:exp_minus"},
            ],
            [
                {"text": "📦 +1 عنصر Digest", "callback_data": "owner:digest_plus"},
                {"text": "📦 -1 عنصر Digest", "callback_data": "owner:digest_minus"},
            ],
        ]
    }


def send_message(token: str, chat_id: str, text: str, reply_markup: Optional[Dict[str, Any]] = None, disable_preview: bool = True) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_request(token, "sendMessage", payload)


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
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_request(token, "editMessageText", payload)


def answer_callback(token: str, callback_query_id: str, text: str, show_alert: bool = False) -> None:
    tg_request(token, "answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text": text[:200],
        "show_alert": show_alert,
    })


def get_updates(token: str, offset: int) -> List[Dict[str, Any]]:
    return tg_request(token, "getUpdates", {
        "offset": offset,
        "timeout": 0,
        "allowed_updates": ["message", "callback_query"],
    })


def subscribe_private(state: Dict[str, Any], user: Dict[str, Any], chat_id: str) -> None:
    state["subscribers"][chat_id] = {
        "active": True,
        "user_id": str(user["id"]),
        "added_at": utc_now_ts(),
        "mode": "private",
    }


def worth_to_float(worth: str) -> float:
    if not worth or worth in ("N/A",):
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


def active_target_chat_ids(state: Dict[str, Any]) -> List[str]:
    cfg = state["config"]
    chats = []
    if cfg.get("send_to_private_subscribers", True):
        chats.extend([cid for cid, info in state["subscribers"].items() if info.get("active")])
    chats.extend(cfg.get("broadcast_chat_ids", []))
    out = []
    seen = set()
    for c in chats:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def giveaway_is_expiring(item: Dict[str, str], window_hours: int) -> bool:
    dt = parse_dt(item["end_date"])
    if not dt:
        return False
    diff = int(dt.timestamp()) - utc_now_ts()
    return 0 < diff <= int(window_hours) * 3600


def build_top_text(state: Dict[str, Any]) -> str:
    ranking = []
    for uid, prof in state["user_profiles"].items():
        ranking.append((len(state["claims"].get(uid, [])), user_label(prof, uid)))
    ranking.sort(key=lambda x: (-x[0], x[1].lower()))
    if not ranking:
        return "لا توجد إحصاءات بعد."
    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>لوحة الشرف VIP</b>\n"]
    for i, (count, name) in enumerate(ranking[:15], start=1):
        prefix = medals[i-1] if i <= 3 else f"{i}."
        lines.append(f"{prefix} <b>{escape(name)}</b> — {count} عرض")
    return "\n".join(lines)


def owner_stats_text(state: Dict[str, Any]) -> str:
    cfg = state["config"]
    subs_total = len(state["subscribers"])
    subs_active = sum(1 for x in state["subscribers"].values() if x.get("active"))
    return (
        f"{escape(cfg.get('vip_badge', 'VIP'))} <b>لوحة المالك</b>\n\n"
        f"المشتركون الكل: <b>{subs_total}</b>\n"
        f"المشتركون النشطون: <b>{subs_active}</b>\n"
        f"الإشعارات المرسلة: <b>{state['stats'].get('notifications_sent', 0)}</b>\n"
        f"العروض الجديدة المكتشفة: <b>{state['stats'].get('new_giveaways_detected', 0)}</b>\n"
        f"تقارير الاستفادة: <b>{state['stats'].get('claim_reports', 0)}</b>\n"
        f"تنبيهات القرب: <b>{state['stats'].get('near_expiry_alerts_sent', 0)}</b>\n"
        f"Digest messages: <b>{state['stats'].get('digest_messages_sent', 0)}</b>\n"
        f"نافذة القرب: <b>{cfg.get('expiring_window_hours', 12)} ساعة</b>\n"
        f"حجم الـ Digest: <b>{cfg.get('digest_max_items', 5)}</b>\n"
        f"عدد شاتات البث: <b>{len(cfg.get('broadcast_chat_ids', []))}</b>\n"
    )


def prefs_text(profile: Dict[str, Any], user_id: str) -> str:
    prefs = profile.get("prefs", default_pref())
    p_list = ", ".join(prefs.get("platform_whitelist", [])) or "الكل"
    t_list = ", ".join(prefs.get("type_whitelist", [])) or "الكل"
    return (
        f"⚙️ <b>تفضيلات {escape(user_label(profile, user_id))}</b>\n\n"
        f"المنصات المسموحة: <b>{escape(p_list)}</b>\n"
        f"الأنواع المسموحة: <b>{escape(t_list)}</b>\n"
        f"الحالة: <b>{'Active فقط' if prefs.get('only_active', True) else 'الكل'}</b>\n"
        f"أقل قيمة بالدولار: <b>{prefs.get('min_worth_usd', 0)}</b>\n"
        f"كتم تنبيهات القرب: <b>{'نعم' if prefs.get('mute_expiring', False) else 'لا'}</b>\n"
        f"Digest فقط: <b>{'نعم' if prefs.get('digest_only', False) else 'لا'}</b>\n\n"
        f"<b>أوامر التخصيص:</b>\n"
        f"/platform steam,epic games\n"
        f"/type game,loot,beta\n"
        f"/minworth 10\n"
        f"/digestonly on|off\n"
        f"/muteexpiring on|off\n"
        f"/resetprefs"
    )


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
        f"تم تفعيل اشتراكك بنجاح.\n"
        f"هذه نسخة VIP: تنبيهات فورية، تنبيهات قرب الانتهاء، تفضيلات مخصصة، ولوحات تحكم أنيقة.\n\n"
        f"<b>الأوامر:</b>\n"
        f"/start\n/stop\n/help\n/me\n/top\n/panel\n"
        f"/setname اسمك\n/platform steam,epic games\n/type game,loot,beta\n"
        f"/minworth 10\n/digestonly on|off\n/muteexpiring on|off\n/resetprefs\n"
        f"/owner للمالك والأدمن\n"
    )
    send_message(token, chat_id, text, reply_markup=build_panel_markup())


def process_stop(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    chat_id = str(message["chat"]["id"])
    if chat_id in state["subscribers"]:
        state["subscribers"][chat_id]["active"] = False
    send_message(token, chat_id, "تم إيقاف الإشعارات. أرسل /start لإعادة التفعيل.")


def process_help(token: str, state: Dict[str, Any], message: Dict[str, Any]) -> None:
    chat_id = str(message["chat"]["id"])
    text = (
        f"{escape(state['config'].get('vip_badge', '💎 VIP'))} <b>شرح النسخة VIP</b>\n\n"
        f"• إشعار لكل Giveaway جديد.\n"
        f"• تنبيه قرب الانتهاء.\n"
        f"• تفضيلات لكل مستخدم: منصة/نوع/قيمة دنيا.\n"
        f"• Digest اختياري لمن يريد إشعارات مجمعة فقط.\n"
        f"• لوحة مالك وأدمن للبث والتحكم.\n\n"
        f"⚠️ <b>قيود دقيقة:</b>\n"
        f"• لا يمكن تتبع الضغط على رابط URL الخارجي نفسه عبر Telegram فقط.\n"
        f"• لا يمكن معرفة من أخذ اللعبة فعليًا من Steam/Epic/GOG عبر GamerPower API.\n"
        f"• الإحصاءات تعتمد على التفاعل داخل البوت."
    )
    send_message(token, chat_id, text)


def process_setname(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    user = message["from"]
    profile = ensure_profile(state, user)
    chat_id = str(message["chat"]["id"])
    new_name = text[len("/setname"):].strip()
    if not new_name:
        send_message(token, chat_id, "اكتب هكذا:\n<code>/setname DevChaggy0x1</code>")
        return
    profile["display_name"] = new_name[:50]
    send_message(token, chat_id, f"تم حفظ الاسم: <b>{escape(profile['display_name'])}</b>")


def process_me(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    uid = str(user["id"])
    claimed = state["claims"].get(uid, [])
    text = (
        f"👤 <b>ملفك VIP</b>\n\n"
        f"الاسم: <b>{escape(user_label(profile, uid))}</b>\n"
        f"المعرف: <b>{escape('@' + profile['username'] if profile.get('username') else 'غير موجود')}</b>\n"
        f"عدد العروض المسجلة: <b>{len(claimed)}</b>\n"
        f"ضغطات الأزرار الداخلية: <b>{profile.get('clicked_count', 0)}</b>\n"
        f"التسجيل: <b>{profile.get('registered_at', 'N/A')}</b>\n\n"
        f"{prefs_text(profile, uid)}"
    )
    send_message(token, chat_id, text)


def process_top(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    send_message(token, str(message["chat"]["id"]), build_top_text(state))


def process_panel(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    text = "🎛 <b>لوحتك VIP</b>\n\nاختر من الأزرار للوصول السريع."
    send_message(token, str(message["chat"]["id"]), text, reply_markup=build_panel_markup())


def process_owner(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    uid = str(message["from"]["id"])
    chat_id = str(message["chat"]["id"])
    if not is_admin_or_owner(state, uid):
        send_message(token, chat_id, "هذه اللوحة للمالك أو الأدمن فقط.")
        return
    send_message(token, chat_id, owner_stats_text(state), reply_markup=build_owner_panel(state))


def process_addadmin(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    uid = str(message["from"]["id"])
    chat_id = str(message["chat"]["id"])
    if not is_owner(state, uid):
        send_message(token, chat_id, "هذا الأمر للمالك فقط.")
        return
    arg = text[len("/addadmin"):].strip()
    if not arg:
        send_message(token, chat_id, "اكتب: <code>/addadmin 123456789</code>")
        return
    if arg not in state["config"]["admin_ids"]:
        state["config"]["admin_ids"].append(arg)
    send_message(token, chat_id, f"تمت إضافة الأدمن: <b>{escape(arg)}</b>")


def process_removeadmin(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    uid = str(message["from"]["id"])
    chat_id = str(message["chat"]["id"])
    if not is_owner(state, uid):
        send_message(token, chat_id, "هذا الأمر للمالك فقط.")
        return
    arg = text[len("/removeadmin"):].strip()
    if not arg:
        send_message(token, chat_id, "اكتب: <code>/removeadmin 123456789</code>")
        return
    state["config"]["admin_ids"] = [x for x in state["config"]["admin_ids"] if x != arg]
    send_message(token, chat_id, f"تم حذف الأدمن: <b>{escape(arg)}</b>")


def process_platform(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    arg = text[len("/platform"):].strip()
    if not arg:
        profile["prefs"]["platform_whitelist"] = []
        send_message(token, chat_id, "تم تصفير فلتر المنصات. الآن كل المنصات مسموحة.")
        return
    values = [x.strip() for x in arg.split(",") if x.strip()]
    profile["prefs"]["platform_whitelist"] = values
    send_message(token, chat_id, f"تم حفظ المنصات: <b>{escape(', '.join(values))}</b>")


def process_type(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    arg = text[len("/type"):].strip()
    if not arg:
        profile["prefs"]["type_whitelist"] = []
        send_message(token, chat_id, "تم تصفير فلتر الأنواع. الآن كل الأنواع مسموحة.")
        return
    values = [x.strip() for x in arg.split(",") if x.strip()]
    profile["prefs"]["type_whitelist"] = values
    send_message(token, chat_id, f"تم حفظ الأنواع: <b>{escape(', '.join(values))}</b>")


def process_minworth(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    arg = text[len("/minworth"):].strip()
    try:
        value = float(arg)
    except Exception:
        send_message(token, chat_id, "اكتب رقمًا صحيحًا، مثال:\n<code>/minworth 10</code>")
        return
    profile["prefs"]["min_worth_usd"] = max(value, 0.0)
    send_message(token, chat_id, f"تم حفظ الحد الأدنى للقيمة: <b>{value}</b>$")


def on_off_value(arg: str) -> Optional[bool]:
    arg = arg.strip().lower()
    if arg in ("on", "yes", "1", "true"):
        return True
    if arg in ("off", "no", "0", "false"):
        return False
    return None


def process_digestonly(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    arg = text[len("/digestonly"):].strip()
    val = on_off_value(arg)
    if val is None:
        send_message(token, chat_id, "استخدم on أو off.\n<code>/digestonly on</code>")
        return
    profile["prefs"]["digest_only"] = val
    send_message(token, chat_id, f"تم ضبط Digest only على: <b>{'ON' if val else 'OFF'}</b>")


def process_muteexpiring(state: Dict[str, Any], token: str, message: Dict[str, Any], text: str) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    arg = text[len("/muteexpiring"):].strip()
    val = on_off_value(arg)
    if val is None:
        send_message(token, chat_id, "استخدم on أو off.\n<code>/muteexpiring on</code>")
        return
    profile["prefs"]["mute_expiring"] = val
    send_message(token, chat_id, f"تم ضبط كتم تنبيهات القرب على: <b>{'ON' if val else 'OFF'}</b>")


def process_resetprefs(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = str(message["chat"]["id"])
    profile = ensure_profile(state, user)
    profile["prefs"] = default_pref()
    send_message(token, chat_id, "تمت إعادة التفضيلات للوضع الافتراضي.")


def process_message_update(state: Dict[str, Any], token: str, message: Dict[str, Any]) -> None:
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
        process_help(token, state, message)
    elif text.startswith("/me"):
        process_me(state, token, message)
    elif text.startswith("/top"):
        process_top(state, token, message)
    elif text.startswith("/panel"):
        process_panel(state, token, message)
    elif text.startswith("/owner"):
        process_owner(state, token, message)
    elif text.startswith("/setname"):
        process_setname(state, token, message, text)
    elif text.startswith("/addadmin"):
        process_addadmin(state, token, message, text)
    elif text.startswith("/removeadmin"):
        process_removeadmin(state, token, message, text)
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


def handle_panel_callback(state: Dict[str, Any], token: str, callback_query: Dict[str, Any], action: str) -> None:
    cqid = callback_query["id"]
    msg = callback_query.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id"))
    message_id = msg.get("message_id")
    user = callback_query["from"]
    profile = ensure_profile(state, user)
    uid = str(user["id"])

    if action == "me":
        edit_message(token, chat_id, message_id, (
            f"👤 <b>ملفك VIP</b>\n\n"
            f"الاسم: <b>{escape(user_label(profile, uid))}</b>\n"
            f"عدد العروض المسجلة: <b>{len(state['claims'].get(uid, []))}</b>\n\n"
            f"{prefs_text(profile, uid)}"
        ), reply_markup=build_panel_markup())
        answer_callback(token, cqid, "تم فتح ملفك.")
    elif action == "top":
        edit_message(token, chat_id, message_id, build_top_text(state), reply_markup=build_panel_markup())
        answer_callback(token, cqid, "تم فتح الترتيب.")
    elif action == "prefs":
        edit_message(token, chat_id, message_id, prefs_text(profile, uid), reply_markup=build_panel_markup())
        answer_callback(token, cqid, "تم فتح التفضيلات.")
    elif action == "help":
        answer_callback(token, cqid, "تم فتح المساعدة.")
        send_message(token, chat_id, "استخدم /help لشرح كامل.")


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
        subs_total = len(state["subscribers"])
        subs_active = sum(1 for x in state["subscribers"].values() if x.get("active"))
        answer_callback(token, cqid, f"النشطون {subs_active} / الكل {subs_total}", show_alert=True)
    elif action == "add_chat":
        if chat_id not in cfg["broadcast_chat_ids"]:
            cfg["broadcast_chat_ids"].append(chat_id)
        answer_callback(token, cqid, "تمت إضافة هذا الشات للبث.")
    elif action == "remove_chat":
        cfg["broadcast_chat_ids"] = [x for x in cfg["broadcast_chat_ids"] if x != chat_id]
        answer_callback(token, cqid, "تم حذف الشات من البث.")
    elif action == "exp_plus":
        cfg["expiring_window_hours"] = min(int(cfg.get("expiring_window_hours", 12)) + 1, 72)
        answer_callback(token, cqid, f"نافذة القرب: {cfg['expiring_window_hours']} ساعة")
    elif action == "exp_minus":
        cfg["expiring_window_hours"] = max(int(cfg.get("expiring_window_hours", 12)) - 1, 1)
        answer_callback(token, cqid, f"نافذة القرب: {cfg['expiring_window_hours']} ساعة")
    elif action == "digest_plus":
        cfg["digest_max_items"] = min(int(cfg.get("digest_max_items", 5)) + 1, 20)
        answer_callback(token, cqid, f"حجم Digest: {cfg['digest_max_items']}")
    elif action == "digest_minus":
        cfg["digest_max_items"] = max(int(cfg.get("digest_max_items", 5)) - 1, 1)
        answer_callback(token, cqid, f"حجم Digest: {cfg['digest_max_items']}")

    edit_message(token, chat_id, message_id, owner_stats_text(state), reply_markup=build_owner_panel(state))


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
        send_message(token, chat_id, f"🎉 تم تسجيل هذا العرض لك.\nإجمالي ما سجلته: <b>{len(claims)}</b>")
    else:
        answer_callback(token, cqid, "هذا العرض مسجل عندك بالفعل.")


def process_updates(state: Dict[str, Any], token: str) -> None:
    updates = get_updates(token, int(state.get("telegram_offset", 0)))
    for upd in updates:
        state["telegram_offset"] = int(upd["update_id"]) + 1
        if "message" in upd:
            process_message_update(state, token, upd["message"])
        elif "callback_query" in upd:
            data = upd["callback_query"].get("data", "")
            if data.startswith("claim:"):
                process_callback_claim(state, token, upd["callback_query"], data.split(":", 1)[1].strip())
            elif data.startswith("panel:"):
                handle_panel_callback(state, token, upd["callback_query"], data.split(":", 1)[1].strip())
            elif data.startswith("owner:"):
                handle_owner_callback(state, token, upd["callback_query"], data.split(":", 1)[1].strip())


def user_target_should_receive(state: Dict[str, Any], chat_id: str, item: Dict[str, str], expiring: bool = False) -> bool:
    sub = state["subscribers"].get(chat_id)
    if not sub:
        return True  # channels/groups in broadcast list use global behavior
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
        return 0
    new_items = [x for x in items if x["id"] not in seen_ids]
    targets = active_target_chat_ids(state)
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

    # only for users who enabled digest_only, plus broadcast chats get no digest by default
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


def validate_env() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise BotError("TELEGRAM_BOT_TOKEN is missing")
    return token


def main() -> int:
    token = validate_env()
    state = load_state()
    process_updates(state, token)
    items = [normalize_giveaway(x) for x in fetch_giveaways()]
    new_count = send_new_giveaways(state, token, items)
    exp_count = send_expiring_alerts(state, token, items)
    digest_count = send_digest(state, token, items)
    save_state(state)
    print(f"Done. new={new_count}, expiring={exp_count}, digest={digest_count}")
    print("Important: external URL button clicks are not trackable without your own redirect/service.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
