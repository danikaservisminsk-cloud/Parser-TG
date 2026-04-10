import asyncio
import base64
import json
import logging
import os
import re
import urllib.request
from datetime import datetime, timezone, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    ChannelPrivateError,
    ChatForbiddenError,
    UserDeactivatedError,
    AuthKeyUnregisteredError,
)

# ══════════════════════════════════════════════════════════════════════════════
# Конфигурация
# ══════════════════════════════════════════════════════════════════════════════

API_ID = int(os.environ.get('TG_API_ID', '0'))
API_HASH = os.environ.get('TG_API_HASH', '')
SESSION_STRING = os.environ.get('TG_SESSION', '')

# SPREADSHEET_ID: один ID или несколько через запятую — "id1,id2,id3"
SPREADSHEET_IDS_RAW = os.environ.get('SPREADSHEET_ID', '')
SPREADSHEET_IDS = [s.strip() for s in SPREADSHEET_IDS_RAW.split(',') if s.strip()]

GOOGLE_CREDENTIALS_BASE64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
LOOKBACK_MINUTES = int(os.environ.get('LOOKBACK_MINUTES', '35'))

# Пауза между каналами (секунды). Снижает риск FloodWait.
INTER_CHANNEL_SLEEP = float(os.environ.get('INTER_CHANNEL_SLEEP', '2.0'))

# Максимальный FloodWait (сек), который мы готовы ждать прямо сейчас.
# Если больше — канал пропускается, прогон продолжается.
MAX_FLOOD_WAIT = int(os.environ.get('MAX_FLOOD_WAIT', '120'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_gc():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet(gc, spreadsheet_id):
    return gc.open_by_key(spreadsheet_id)


def get_settings(ss):
    """Возвращает (keywords_enabled, keywords, tg_token, target_chats).

    Структура листа «Настройки»:
      Строка 1 (idx 0): [Настройки бота | | | Ключевые слова | вкл/выкл(чекбокс)]
      Строка 2 (idx 1): [TG-бот | <токен> | | <keyword> | ...]
      Строка 3 (idx 2): [Чаты   | <chat_id> | | <keyword> | ...]
      Строка 4+        : [       | <chat_id> | | <keyword> | ...]

    Токен бота       — B2  (data[1][1])
    Чекбокс ключевых — E1  (data[0][4])  TRUE = включены
    Ключевые слова   — колонка D (idx 3), строки 2+ (idx 1+)
    Chat_id          — колонка B (idx 1), строки 3+ (idx 2+)
    """
    try:
        sheet = ss.worksheet('Настройки')
        data = sheet.get_all_values()

        keywords_enabled = (
            str(data[0][4]).upper() == 'TRUE'
            if data and len(data[0]) > 4
            else False
        )

        keywords = []
        for row in data[1:]:
            if len(row) > 3 and row[3].strip():
                keywords.append(row[3].strip())

        token = (
            str(data[1][1]).strip()
            if len(data) > 1 and len(data[1]) > 1
            else ''
        )

        # Чаты: колонка B (idx 1), строки 3+ (idx 2+)
        # Защита: не добавляем строку с токеном (строка 2) в список чатов
        target_chats = []
        for row in data[2:]:
            chat_id = str(row[1]).strip() if len(row) > 1 else ''
            if chat_id and chat_id != token:
                target_chats.append(chat_id)

        return keywords_enabled, keywords, token, target_chats

    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e))
        return False, [], '', []


def get_channels(ss):
    """Читает лист «Каналы». Колонки: Канал | Последний пост | Статус."""
    try:
        sheet = ss.worksheet('Каналы')
        data = sheet.get_all_values()
        channels = []
        for i, row in enumerate(data[1:], start=2):
            if not row or not row[0].strip():
                continue
            username = extract_username(row[0].strip())
            if not username:
                continue
            last_link = row[1].strip() if len(row) > 1 else ''
            channels.append({'username': username, 'last_link': last_link, 'row': i})
        return channels
    except Exception as e:
        log.error('Ошибка чтения каналов: ' + str(e))
        return []


def _sheets_retry(fn, retries=3, delay=5):
    """Выполняет fn() с повторными попытками при ошибках Google Sheets API."""
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            log.warning(f'Sheets retry {attempt}/{retries}: {e}')
            if attempt < retries:
                time.sleep(delay * attempt)
    log.error(f'Sheets: все {retries} попытки исчерпаны')


def update_channel(ss, row, last_link, status):
    _sheets_retry(lambda: ss.worksheet('Каналы').update(
        [[last_link, status]], f'B{row}:C{row}'
    ))


def write_posts(ss, posts):
    if not posts:
        return
    rows = [[
        p['date'].strftime('%Y-%m-%d %H:%M:%S'),
        p['chat_name'],
        p['author_name'],
        p['author_link'],
        p['link'],
        p['text'],
    ] for p in posts]
    _sheets_retry(lambda: ss.worksheet('Посты').append_rows(
        rows, value_input_option='USER_ENTERED'
    ))
    log.info('Записано постов: ' + str(len(rows)))


def write_log(ss, level, message):
    safe = str(message)
    if safe and safe[0] in '=+-@':
        safe = "'" + safe
    _sheets_retry(lambda: ss.worksheet('Логи').append_row(
        [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, safe],
        value_input_option='USER_ENTERED',
    ))


# ══════════════════════════════════════════════════════════════════════════════
# Telegram send
# ══════════════════════════════════════════════════════════════════════════════

def send_to_telegram(posts, tg_token, target_chats):
    """Отправляет посты в плоский список чатов."""
    if not posts or not tg_token or not target_chats:
        return

    for p in posts:
        parts = ['📢 ' + p['chat_name']]
        if p.get('author_name'):
            author_str = p['author_name']
            if p.get('author_link'):
                author_str += ' — ' + p['author_link']
            parts.append('👤 ' + author_str)
        parts.append('')
        parts.append(p['text'])
        parts.append('')
        parts.append('🔗 ' + p['link'])

        body = '\n'.join(parts)
        if len(body) > 4000:
            body = body[:4000] + '...'

        for chat_id in target_chats:
            for attempt in range(1, 4):
                try:
                    url = f'https://api.telegram.org/bot{tg_token}/sendMessage'
                    data = json.dumps({
                        'chat_id': chat_id,
                        'text': body,
                        'disable_web_page_preview': False,
                    }).encode('utf-8')
                    req = urllib.request.Request(
                        url, data=data,
                        headers={'Content-Type': 'application/json'},
                    )
                    urllib.request.urlopen(req, timeout=10)
                    time.sleep(0.5)
                    break
                except Exception as e:
                    log.error(
                        f'Ошибка отправки TG в {chat_id} '
                        f'(попытка {attempt}/3): {e} | текст: {body[:100]}'
                    )
                    time.sleep(1 * attempt)


# ══════════════════════════════════════════════════════════════════════════════
# Утилиты
# ══════════════════════════════════════════════════════════════════════════════

def extract_username(raw):
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([a-zA-Z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^[a-zA-Z0-9_]+$', raw):
        return raw
    if re.match(r'^-?\d+$', raw):
        return raw
    return None


def extract_post_id(link):
    m = re.search(r'/(\d+)$', link)
    return int(m.group(1)) if m else 0


def build_link(chat, msg_id):
    username = getattr(chat, 'username', None)
    if username:
        return f'https://t.me/{username}/{msg_id}'
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    return f'https://t.me/c/{chat_id}/{msg_id}'


def get_author_info(msg):
    try:
        if not msg.sender:
            return '', ''
        sender = msg.sender
        first = getattr(sender, 'first_name', '') or ''
        last = getattr(sender, 'last_name', '') or ''
        username = getattr(sender, 'username', '') or ''
        full_name = (first + ' ' + last).strip()
        author_link = f'https://t.me/{username}' if username else ''
        return full_name, author_link
    except Exception:
        return '', ''


def matches_keywords(text, keywords):
    if not text or not keywords:
        return False
    text_lower = text.lower()
    for kw in keywords:
        kw_lower = kw.lower().strip().rstrip('*')
        if kw_lower and kw_lower in text_lower:
            return True
    return False


def group_messages(messages):
    """Схлопывает медиагруппы (альбомы) в один пост. Одиночные медиа без текста отбрасываются."""
    grouped = {}
    singles = []

    for msg in messages:
        gid = getattr(msg, 'grouped_id', None)
        if gid:
            grouped.setdefault(gid, []).append(msg)
        else:
            singles.append(msg)

    result = []

    for msg in singles:
        text = msg.text or msg.message or ''
        if hasattr(msg, 'caption') and msg.caption:
            text = msg.caption
        text = ' '.join(text.split())
        if text.strip():
            result.append((msg, text))

    for gid, msgs in grouped.items():
        msgs_sorted = sorted(msgs, key=lambda m: m.id)
        combined_text = ''
        for m in msgs_sorted:
            t = m.text or m.message or ''
            if hasattr(m, 'caption') and m.caption:
                t = m.caption
            t = ' '.join(t.split())
            if t.strip():
                combined_text = t
                break
        if combined_text.strip():
            result.append((msgs_sorted[0], combined_text))

    result.sort(key=lambda x: x[0].id)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Безопасная итерация сообщений
# ══════════════════════════════════════════════════════════════════════════════

async def safe_iter_messages(client, chat_username, limit, last_post_id, since):
    """
    Читает сообщения канала с защитой от FloodWait.
    При FloodWait <= MAX_FLOOD_WAIT — ждёт и делает одну повторную попытку.
    При FloodWait > MAX_FLOOD_WAIT — пробрасывает исключение (канал будет пропущен).
    """
    async def _fetch():
        msgs = []
        async for msg in client.iter_messages(chat_username, limit=limit):
            if last_post_id > 0:
                if msg.id <= last_post_id:
                    break
            else:
                if msg.date < since:
                    break
            msgs.append(msg)
        return msgs

    try:
        return await _fetch()
    except FloodWaitError as e:
        wait = e.seconds
        if wait > MAX_FLOOD_WAIT:
            log.error(f'{chat_username} | FloodWait {wait}s > MAX={MAX_FLOOD_WAIT}s — пропускаем')
            raise
        log.warning(f'{chat_username} | FloodWait {wait}s при чтении — ждём и повторяем')
        await asyncio.sleep(wait + 3)
        try:
            return await _fetch()
        except FloodWaitError as e2:
            log.error(f'{chat_username} | FloodWait повторно {e2.seconds}s — пропускаем')
            raise


# ══════════════════════════════════════════════════════════════════════════════
# Обработка одной таблицы
# ══════════════════════════════════════════════════════════════════════════════

async def process_spreadsheet(client, ss, ss_id):
    log.info(f'=== Таблица {ss_id} ===')

    keywords_enabled, keywords, tg_token, target_chats = get_settings(ss)
    channels = get_channels(ss)

    log.info(
        f'Каналов: {len(channels)} | '
        f'Ключи: {"ВКЛ (" + str(len(keywords)) + " шт)" if keywords_enabled else "ВЫКЛ"} | '
        f'Чатов для отправки: {len(target_chats)}'
    )

    if not channels:
        log.warning('Нет каналов в листе Каналы')
        write_log(ss, 'WARN', 'Нет каналов в листе Каналы')
        return

    all_new_posts = []
    total_new = 0
    total_saved = 0

    write_log(
        ss, 'INFO',
        f'ПРОГОН НАЧАТ | чатов: {len(channels)} | '
        f'ключи: {"ВКЛ (" + str(len(keywords)) + " шт)" if keywords_enabled else "ВЫКЛ"}',
    )

    for ch in channels:
        chat_username = ch['username']
        last_link = ch['last_link']
        last_post_id = extract_post_id(last_link) if last_link else 0
        row = ch['row']
        fetch_limit = 30 if last_post_id > 0 else 50

        try:
            since = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

            raw_messages = await safe_iter_messages(
                client, chat_username, fetch_limit, last_post_id, since
            )

            if not raw_messages:
                update_channel(ss, row, last_link, '✅ Нет новых сообщений')
                log.info(f'{chat_username} | новых: 0')
                await asyncio.sleep(INTER_CHANNEL_SLEEP)
                continue

            # Получаем chat из первого сообщения — без отдельного get_entity запроса
            try:
                chat = await raw_messages[0].get_chat()
            except Exception as e:
                log.warning(f'{chat_username} | get_chat() упал: {e} — пропускаем')
                update_channel(ss, row, last_link, f'❌ get_chat ошибка: {str(e)[:50]}')
                write_log(ss, 'ERROR', f'{chat_username} | get_chat: {str(e)[:100]}')
                await asyncio.sleep(INTER_CHANNEL_SLEEP)
                continue

            chat_name = (
                getattr(chat, 'title', None)
                or getattr(chat, 'username', None)
                or str(chat_username)
            )

            raw_messages.sort(key=lambda m: m.id)
            raw_messages = [m for m in raw_messages if m.action is None]
            grouped = group_messages(raw_messages)

            new_msgs_count = len(raw_messages)
            saved_msgs = []
            new_last_link = build_link(chat, raw_messages[-1].id)

            for msg, text in grouped:
                if keywords_enabled and keywords:
                    if not matches_keywords(text, keywords):
                        continue
                author_name, author_link = get_author_info(msg)
                saved_msgs.append({
                    'date': msg.date.replace(tzinfo=None),
                    'chat_name': chat_name,
                    'author_name': author_name,
                    'author_link': author_link,
                    'link': build_link(chat, msg.id),
                    'text': text,
                })

            total_new += new_msgs_count
            total_saved += len(saved_msgs)
            all_new_posts.extend(saved_msgs)

            update_channel(
                ss, row, new_last_link,
                f'✅ Новых: {new_msgs_count} | Записано: {len(saved_msgs)}',
            )
            log.info(
                f'{chat_username} | новых: {new_msgs_count} | '
                f'после группировки: {len(grouped)} | '
                f'в таблицу: {len(saved_msgs)} | '
                f'lastId: {last_post_id or "пусто"}'
            )

        except FloodWaitError as e:
            wait = e.seconds
            if wait <= MAX_FLOOD_WAIT:
                log.warning(f'{chat_username} | FloodWait {wait}s — ждём и повторяем')
                write_log(ss, 'WARN', f'{chat_username} | FloodWait {wait}s — ждём')
                await asyncio.sleep(wait + 3)
                # Повторная попытка после ожидания
                try:
                    raw_messages = await safe_iter_messages(
                        client, chat_username, fetch_limit, last_post_id, since
                    )
                    update_channel(ss, row, last_link, f'⏳ После FloodWait {wait}s — OK')
                    log.info(f'{chat_username} | повтор после FloodWait OK, сообщений: {len(raw_messages)}')
                except Exception as retry_e:
                    log.error(f'{chat_username} | Повтор после FloodWait упал: {retry_e}')
                    update_channel(ss, row, last_link, '❌ Повтор после FloodWait упал')
            else:
                log.error(f'{chat_username} | FloodWait {wait}s > {MAX_FLOOD_WAIT}s — пропускаем')
                update_channel(ss, row, last_link, f'⏳ FloodWait {wait}s — пропущен')
                write_log(ss, 'WARN', f'{chat_username} | FloodWait {wait}s — пропущен')

        except (UsernameInvalidError, UsernameNotOccupiedError):
            log.error(f'{chat_username} | Невалидный username')
            update_channel(ss, row, last_link, '❌ Невалидный username')
            write_log(ss, 'ERROR', f'{chat_username} | Невалидный username')

        except (ChannelPrivateError, ChatForbiddenError):
            log.error(f'{chat_username} | Канал недоступен/приватный')
            update_channel(ss, row, last_link, '❌ Канал недоступен')
            write_log(ss, 'ERROR', f'{chat_username} | Канал недоступен')

        except (UserDeactivatedError, AuthKeyUnregisteredError) as e:
            # Сессия умерла — дальше продолжать бессмысленно
            log.critical(f'СЕССИЯ НЕВАЛИДНА: {e}')
            write_log(ss, 'CRITICAL', f'Сессия невалидна: {e}')
            raise

        except Exception as e:
            log.error(f'{chat_username} | ОШИБКА: {e}')
            update_channel(ss, row, last_link, f'❌ Ошибка: {str(e)[:50]}')
            write_log(ss, 'ERROR', f'{chat_username} | {str(e)[:100]}')
            await asyncio.sleep(2)

        finally:
            # Пауза после каждого канала — всегда, независимо от результата
            await asyncio.sleep(INTER_CHANNEL_SLEEP)

    write_posts(ss, all_new_posts)

    if all_new_posts and tg_token and target_chats:
        log.info(f'Отправляю {len(all_new_posts)} постов в TG...')
        send_to_telegram(all_new_posts, tg_token, target_chats)

    summary = (
        f'[{ss_id}] ПРОГОН ЗАВЕРШЁН | чатов: {len(channels)} | '
        f'новых: {total_new} | записано: {total_saved}'
    )
    log.info(summary)
    write_log(ss, 'INFO', summary)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info('Запуск прогона...')

    if not SPREADSHEET_IDS:
        log.error('SPREADSHEET_ID не задан')
        return

    try:
        gc = get_gc()
        log.info('Google Sheets авторизация OK')
    except Exception as e:
        log.error('Ошибка Google Sheets авторизации: ' + str(e))
        return

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    try:
        await client.start()
        log.info('Telegram подключён')

        for ss_id in SPREADSHEET_IDS:
            try:
                ss = get_spreadsheet(gc, ss_id)
                await process_spreadsheet(client, ss, ss_id)
            except (UserDeactivatedError, AuthKeyUnregisteredError) as e:
                log.critical(f'Сессия невалидна — останавливаем прогон: {e}')
                break
            except Exception as e:
                log.error(f'Критическая ошибка таблицы {ss_id}: {e}')

    finally:
        await client.disconnect()
        log.info('Готово.')


if __name__ == '__main__':
    asyncio.run(main())
