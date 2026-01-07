#!/usr/bin/env python3
"""Тест доступа к Google Drive API."""

import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Загружаем токен
with open('token.json', 'r') as f:
    token_data = json.load(f)

print(f"Token client_id: {token_data.get('client_id', 'N/A')[:20]}...")
print(f"Token scopes: {token_data.get('scopes', [])}")

# Создаём credentials
creds = Credentials.from_authorized_user_file('token.json')
print(f"Credentials valid: {creds.valid}")
print(f"Credentials expired: {creds.expired}")

# Создаём сервис
service = build('drive', 'v3', credentials=creds)

# Тестируем доступ к папке
folder_id = '1NCFqnp94y7mAGE_507FdZgHoGo2fPaBB'  # MAGNIT_AUTO
print(f"\nТестируем доступ к папке: {folder_id}")

try:
    # Пробуем получить метаданные папки
    result = service.files().get(
        fileId=folder_id,
        fields='id, name, mimeType, owners, permissions'
    ).execute()
    print(f"✅ Успех! Папка: {result.get('name')}")
    print(f"   Owners: {result.get('owners')}")
except HttpError as e:
    print(f"❌ HTTP Error: {e.resp.status}")
    print(f"   Reason: {e.reason}")
    print(f"   Content: {e.content.decode('utf-8')}")
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {e}")

# Пробуем просто получить список файлов (без указания папки)
print("\nПробуем получить список файлов в корне Drive:")
try:
    result = service.files().list(
        pageSize=5,
        fields='files(id, name)'
    ).execute()
    files = result.get('files', [])
    print(f"✅ Найдено файлов: {len(files)}")
    for f in files:
        print(f"   - {f['name']}")
except HttpError as e:
    print(f"❌ HTTP Error: {e.resp.status}")
    print(f"   Content: {e.content.decode('utf-8')}")

# Пробуем получить список Shared Drives
print("\nПробуем получить список Shared Drives:")
try:
    result = service.drives().list(pageSize=10).execute()
    drives = result.get('drives', [])
    print(f"✅ Найдено Shared Drives: {len(drives)}")
    for drive in drives:
        print(f"   - {drive['name']} (ID: {drive['id']})")
except HttpError as e:
    print(f"❌ HTTP Error: {e.resp.status}")
    print(f"   Content: {e.content.decode('utf-8')}")
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {e}")
