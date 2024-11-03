from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from os import path, environ
import pickle
import re
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv


load_dotenv()


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly',
          'https://www.googleapis.com/auth/spreadsheets']


def format_datetime(datetime_str):
    parsed_datetime = datetime.strptime(datetime_str, "%a, %d %b %Y %H:%M:%S %z")
    formatted_datetime = parsed_datetime.strftime("%a, %d %b %Y %I:%M %p")
    return formatted_datetime


def get_gmail_service():
    """Get or create Gmail API service."""
    creds = None
    if path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds)


def get_sheets_service():
    """Get or create Google Sheets API service."""
    creds = None

    if path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    return build('sheets', 'v4', credentials=creds)


def parse_transaction_email(message):
    """Parse transaction email for card, amount and narration."""
    result = {
        'card': None,
        'amount': None,
        'narration': None
    }
    
    card_patterns = [
        r"(?i)(?:your\s+)?(\w+\s+Credit\s+Card).*?ending\s+(\d{4})", 
        r"(?i)(?:your\s+)?(\w+\s+Bank\s+Credit\s+Card).*?XX(\d{4})", 
        r"(?i)(?:your\s+)?(\w+\s+Bank\s+Credit\s+Card).*?(\d{4})", 
        r"(?i)(?:your\s+)?(\w+\s+Credit\s+Card).*?(\d{4})"
    ]
    amount_patterns = [
        r'(?:transaction|spent|charge|payment|debited)\s+(?:of\s+)?(?:Rs\.|INR)\s*(\d+(?:,\d+)*(?:\.\d{2})?)', 
        r'(?:Rs\.|INR)\s*(\d+(?:,\d+)*(?:\.\d{2})?)\s+(?:spent|debited|transaction|has been done)',
        r'(?:Rs\.|INR)\s*(\d+(?:,\d+)*(?:\.\d{2})?)' 
    ]
    narration_patterns = [
        r'at\s+([^.]+?)\s+on\s+\d', 
        r'Info:\s+([^.]+)'
    ]
 
    for pattern in card_patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            card_name = match.group(1).strip()
            card_number = match.group(2)
            result['card'] = f"{card_name} - {card_number}"
            break
    
    for pattern in amount_patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            result['amount'] = float(match.group(1).replace(',', ''))
            break

    for pattern in narration_patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            result['narration'] = match.group(1).strip()
            break
    
    return result
   

def get_transaction_emails(service, bank_keywords=None):
    """Fetch and parse transaction emails."""
    if bank_keywords is None:
        bank_keywords = ['Transaction alert', 'Transaction Alert']
    
    start_date = "2024/10/01" 
    end_date = "2024/10/31"
    query = ' OR '.join(f'subject:({keyword})' for keyword in bank_keywords)
    query = f"({query}) after:{start_date} before:{end_date}"

    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    
    transactions = []
    
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
        payload = msg['payload']
        
        headers = payload.get('headers')
        date = next(header['value'] for header in headers if header['name'] == 'Date')     

        transactions.append({
            'datetime': format_datetime(date),
            **parse_transaction_email(msg['snippet'])
        })
    
    return transactions


def update_spreadsheet(service, spreadsheet_id, transactions):
    """Update Google Sheet with transactions."""
    df = pd.DataFrame(transactions)
    values = [df.columns.values.tolist()] + df.values.tolist()
    
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range='Sheet1').execute()
    existing_rows = len(result.get('values', []))

    if existing_rows:
        body = {
            'values': values[1:]
        }
    else:
        body = {
            'values': values,
        }

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f'Sheet1!A{existing_rows + (2 if existing_rows else 1)}',
        valueInputOption='RAW',
        body=body
    ).execute()


def main():
    gmail_service = get_gmail_service()
    sheets_service = get_sheets_service()
    
    SPREADSHEET_ID = environ.get('GOOGLE_SHEET_ID')
    
    transactions = get_transaction_emails(gmail_service)

    update_spreadsheet(sheets_service, SPREADSHEET_ID, transactions)
    
    print(f'Processed {len(transactions)} transactions')


if __name__ == '__main__':
    main()
