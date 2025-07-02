import os
import gspread
import logging
import base64
import time
from datetime import datetime, timedelta, timezone
import requests
from oauth2client.service_account import ServiceAccountCredentials

import os
import gspread
import logging
import base64
from datetime import datetime, timedelta, timezone
import requests
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_credentials():
    encoded_creds = os.getenv("CREDENTIALS_JSON")
    if not encoded_creds:
        raise Exception("❌ Переменная окружения CREDENTIALS_JSON не найдена.")
    decoded_creds = base64.b64decode(encoded_creds).decode("utf-8")
    with open("credentials.json", "w") as f:
        f.write(decoded_creds)

def main():
    save_credentials()

    SPREADSHEET_ID = '1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y'
    SOURCE_SHEET = 'unique drivers main'
    TARGET_SHEET = 'NO_REQUIRED_TAXPAYER_STATE'

    USE_DESK_TICKET_URL = 'https://api.usedesk.ru/create/ticket'
    USE_DESK_COMMENT_URL = 'https://api.usedesk.ru/create/comment'
    USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

    TELEGRAM_CHAT_ID = "-1001517811601"
    TELEGRAM_THREAD_ID = 8282

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    source_ws = spreadsheet.worksheet(SOURCE_SHEET)
    target_ws = spreadsheet.worksheet(TARGET_SHEET)

    almaty_now = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M:%S")

    source_rows = source_ws.get_all_values()
    source_header = source_rows[0]
    source_data = source_rows[1:]

    try:
        tin_idx = source_header.index("tin")
        name_idx = source_header.index("name")
        esf_idx = source_header.index("Статус ЭСФ")
    except ValueError:
        raise Exception("❌ Не найдены колонки 'tin', 'name' и 'Статус ЭСФ' в исходной таблице")

    target_header = source_header + ["Время добавления", "Обновлено", "UseDesk", "Telegram"]
    target_rows = target_ws.get_all_values()
    if not target_rows or target_rows[0] != target_header:
        logger.info("⚙️ Обновляем заголовок...")
        target_ws.update("A1", [target_header])
        target_rows = target_ws.get_all_values()

    target_tin_map = {}
    for i, row in enumerate(target_rows[1:], start=2):
        if len(row) > tin_idx:
            target_tin_map[row[tin_idx].strip()] = (i, row)

    added, updated = 0, 0

    for source_row in source_data:
        if len(source_row) <= max(tin_idx, esf_idx, name_idx):
            continue

        tin = source_row[tin_idx].strip()
        name = source_row[name_idx].strip()
        esf_status = source_row[esf_idx].strip()

        if tin in target_tin_map:
            row_num, target_row = target_tin_map[tin]
            old_status = target_row[esf_idx] if esf_idx < len(target_row) else ""

            if old_status != esf_status:
                target_ws.update_cell(row_num, esf_idx + 1, esf_status)
                target_ws.update_cell(row_num, len(source_header) + 2, almaty_now)
                updated += 1
            continue

        if esf_status == "NO_REQUIRED_TAXPAYER_STATE":
            new_row = source_row + [almaty_now, "", "", ""]
            target_ws.append_row(new_row)
            last_row_idx = len(target_ws.get_all_values())
            target_tin_map[tin] = (last_row_idx, new_row)
            added += 1

    for tin, (row_num, row) in target_tin_map.items():
        if len(row) < len(target_header):
            continue

        esf_status = row[esf_idx].strip()
        name = row[name_idx].strip() if name_idx < len(row) else ""
        usedesk_status = row[-2].strip()
        telegram_status = row[-1].strip()

        if esf_status != "NO_REQUIRED_TAXPAYER_STATE":
            continue

        if not usedesk_status:
            ticket_payload = {
                "api_token": USE_DESK_TOKEN,
                "subject": "NO_REQUIRED_TAXPAYER_STATE",
                "message": "автоматическиго",
                "private_comment": "true",
                "client_email": "esfsd@kgd.minfin.gov.kz",
                "from": "user",
                "channel_id": 64326,
                "status": "2"
            }
            response = requests.post(USE_DESK_TICKET_URL, json=ticket_payload)
            try:
                res_json = response.json()
                logger.warning(f"Ответ create/ticket: {res_json}")
                time.sleep(5)
                ticket_id = res_json.get("ticket_id") or res_json.get("ticket", {}).get("id")
                if ticket_id:
                    comment_payload = {
                        "api_token": USE_DESK_TOKEN,
                        "ticket_id": ticket_id,
                        "message": (

                            f"<p>Здравствуйте!</p>"
                            f"<p>При подписании ЭСФ у нашего клиента выходит ошибка - <b>NO_REQUIRED_TAXPAYER_STATE</b>.</p>"
                            f"<p>{name}, его ИИН — {tin}</p>"
                            f"<p>Можете исправить, пожалуйста.</p>"
                        ),
                        "cc": ["esfsd@kgd.minfin.gov.kz", "esfsupport@osdkz.com"],
                        "type": "public",
                        "from": "user"
                    }
                    comment_response = requests.post(USE_DESK_COMMENT_URL, json=comment_payload)
                    if comment_response.status_code == 200:
                        ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
                        target_ws.update_cell(row_num, len(target_header) - 1, ticket_url)
                        usedesk_status = ticket_url
                    else:
                        logger.error(f"❌ Ошибка создания комментария: {comment_response.text}")
                else:
                    logger.error(f"❌ ticket_id не найден в ответе UseDesk для {tin}")
            except Exception as e:
                logger.error(f"❌ Ошибка парсинга ответа UseDesk: {e}")

        if usedesk_status and not telegram_status:
            text = (
                f"🚨 Ошибка у клиента:\n"
                f"ИИН: {tin}\n"
                f"Ошибка: NO_REQUIRED_TAXPAYER_STATE\n"
                f"Тикет создан: {usedesk_status}"
            )
            tg_response = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                data={
                    'chat_id': TELEGRAM_CHAT_ID,
                    'text': text,
                    'message_thread_id': TELEGRAM_THREAD_ID
                }
            )
            if tg_response.status_code == 200:
                target_ws.update_cell(row_num, len(target_header), "отправлено")
            else:
                logger.error(f"❌ Ошибка Telegram для {tin}: {tg_response.text}")

    logger.info(f"✅ Готово! Добавлено: {added}, Обновлено: {updated}")

if __name__ == "__main__":
    main()
