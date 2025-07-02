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
    SHEET_NAME = 'NO_REQUIRED_TAXPAYER_STATE'

    USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = "-1001517811601"
    TELEGRAM_THREAD_ID = 8282

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    rows = ws.get_all_values()
    header = rows[0]
    data = rows[1:]

    try:
        tin_idx = header.index("tin")
        usedesk_idx = header.index("UseDesk")
        telegram_idx = header.index("Telegram")
    except ValueError:
        raise Exception("❌ Не найдены нужные колонки в таблице")

    for i, row in enumerate(data, start=2):
        tin = row[tin_idx].strip()
        usedesk_status = row[usedesk_idx].strip() if len(row) > usedesk_idx else ""
        telegram_status = row[telegram_idx].strip() if len(row) > telegram_idx else ""

        if not usedesk_status:
            # Step 1: create ticket with private message
            private_msg = f"Ошибка клиента: NO_REQUIRED_TAXPAYER_STATE\nИИН: {tin}"
            ticket_payload = {
                "api_token": USE_DESK_TOKEN,
                "subject": "NO_REQUIRED_TAXPAYER_STATE",
                "client_email": "djamil21ex@gmail.com",
                "message": private_msg,
                "from": "user",
                "channel_id": 64326,
                "status": "2"
            }

            ticket_resp = requests.post("https://api.usedesk.ru/create/ticket", json=ticket_payload)
            logger.warning(f"Ответ create/ticket: {ticket_resp.text}")

            if ticket_resp.status_code == 200:
                ticket_id = ticket_resp.json().get("ticket_id")
                if ticket_id:
                    ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
                    ws.update_cell(i, usedesk_idx + 1, ticket_url)

                    # Step 2: send public comment with cc
                    public_msg = (
                        f"<p>Здравствуйте!<br><br>При подписании ЭСФ у нашего клиента выходит ошибка — <b>NO_REQUIRED_TAXPAYER_STATE</b>.<br>"
                        f"ИИН клиента — {tin}<br>Просим исправить.<br></p>"
                    )
                    comment_payload = {
                        "api_token": USE_DESK_TOKEN,
                        "ticket_id": ticket_id,
                        "message": public_msg,
                        "type": "public",
                        "from": "user",
                        "cc": ["djamil1ex@gmail.com", "5599881@mail.ru"]
                    }

                    comment_resp = requests.post("https://api.usedesk.ru/create/comment", json=comment_payload)
                    logger.warning(f"Ответ create/comment: {comment_resp.text}")
                else:
                    logger.error(f"❌ ticket_id отсутствует в ответе UseDesk для {tin}")
            else:
                logger.error(f"❌ Ошибка UseDesk: {ticket_resp.status_code} — {ticket_resp.text}")

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
                ws.update_cell(i, telegram_idx + 1, "отправлено")
            else:
                logger.error(f"❌ Ошибка Telegram для {tin}: {tg_response.text}")

    logger.info("✅ Готово!")

if __name__ == "__main__":
    main()
