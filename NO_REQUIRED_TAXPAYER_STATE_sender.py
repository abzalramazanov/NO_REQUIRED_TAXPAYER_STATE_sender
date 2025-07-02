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
    TARGET_SHEET = 'NO_REQUIRED_TAXPAYER_STATE'

    USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = "-1001517811601"
    TELEGRAM_THREAD_ID = 8282

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    target_ws = spreadsheet.worksheet(TARGET_SHEET)
    almaty_now = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M:%S")

    target_rows = target_ws.get_all_values()
    target_header = target_rows[0]
    tin_idx = target_header.index("tin")
    esf_idx = target_header.index("Статус ЭСФ")

    added = 0

    for i, row in enumerate(target_rows[1:], start=2):
        tin = row[tin_idx].strip()
        esf_status = row[esf_idx].strip()
        usedesk_status = row[-2].strip()
        telegram_status = row[-1].strip()

        if esf_status != "NO_REQUIRED_TAXPAYER_STATE" or usedesk_status:
            continue

        # Шаг 1: создаём тикет с приватным комментарием
        ticket_payload = {
            "api_token": USE_DESK_TOKEN,
            "subject": "NO_REQUIRED_TAXPAYER_STATE",
            "message": f"Ошибка клиента: NO_REQUIRED_TAXPAYER_STATE\nИИН: {tin}",
            "client_email": "djamil1ex@gmail.com",
            "from": "user",
            "channel_id": 64326,
            "status": "2",
            "private_comment": True
        }

        ticket_resp = requests.post("https://api.usedesk.ru/create/ticket", json=ticket_payload)
        logger.warning(f"Ответ create/ticket: {ticket_resp.text}")

        if ticket_resp.status_code == 200 and ticket_resp.json().get("ticket_id"):
            ticket_id = ticket_resp.json().get("ticket_id")
            ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
            target_ws.update_cell(i, len(target_header) - 1, ticket_url)
            usedesk_status = ticket_url

            # Шаг 2: добавляем публичный комментарий с копией
            comment_payload = {
                "api_token": USE_DESK_TOKEN,
                "ticket_id": ticket_id,
                "message": (
                    f"<p>Здравствуйте!<br><br>"
                    f"При подписании ЭСФ у нашего клиента выходит ошибка - <b>NO_REQUIRED_TAXPAYER_STATE</b>.<br>"
                    f"ИИН клиента — {tin}<br>"
                    f"Просим исправить.<br></p>"
                ),
                "type": "public",
                "from": "user",
                "cc": ["5599881@mail.ru"]
            }

            comment_resp = requests.post("https://api.usedesk.ru/create/comment", json=comment_payload)
            logger.warning(f"Ответ create/comment: {comment_resp.text}")

            # Шаг 3: отправляем в Telegram
            text = (
                f"🚨 Ошибка у клиента:\n"
                f"ИИН: {tin}\n"
                f"Ошибка: NO_REQUIRED_TAXPAYER_STATE\n"
                f"Тикет создан: {ticket_url}"
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
                target_ws.update_cell(i, len(target_header), "отправлено")
            else:
                logger.error(f"❌ Ошибка Telegram для {tin}: {tg_response.text}")

            added += 1

    logger.info(f"✅ Готово! Добавлено: {added}")

if __name__ == "__main__":
    main()
