# AA_BLE Automation - OAuth2 Helper
"""
Модуль для OAuth2 авторизации через браузер.
Позволяет работать от имени пользователя без Domain-Wide Delegation.
"""

import os
import json
from typing import Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# Scopes для Drive и Sheets (только чтение)
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]


def get_oauth_credentials(
    client_secrets_path: str = 'client_secrets.json',
    token_path: str = 'token.json'
) -> Optional[Credentials]:
    """
    Получение OAuth2 credentials с автоматическим обновлением.
    
    При первом запуске откроет браузер для авторизации.
    После авторизации токен сохраняется в token.json.
    
    Args:
        client_secrets_path: Путь к файлу OAuth client secrets
        token_path: Путь для сохранения токена
        
    Returns:
        Credentials объект или None при ошибке
    """
    creds = None
    
    # Проверяем существующий токен
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception:
            creds = None
    
    # Если токен невалидный или истёк
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Обновляем токен
            try:
                creds.refresh(Request())
            except Exception:
                creds = None
        
        if not creds:
            # Нужна авторизация через браузер
            if not os.path.exists(client_secrets_path):
                print(f"❌ Файл {client_secrets_path} не найден!")
                print("Скачайте OAuth client secrets из Google Cloud Console")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_path, 
                SCOPES
            )
            # prompt='select_account' заставляет Google показать выбор аккаунта
            creds = flow.run_local_server(port=0, prompt='select_account')
        
        # Сохраняем токен
        with open(token_path, 'w') as token_file:
            token_file.write(creds.to_json())
        print(f"✅ Токен сохранён в {token_path}")
    
    return creds


def run_oauth_flow(client_secrets_path: str = 'client_secrets.json'):
    """
    Запуск OAuth2 авторизации вручную.
    Используется для первоначальной настройки или обновления токена.
    """
    print("=" * 50)
    print("OAuth2 Авторизация")
    print("=" * 50)
    
    if not os.path.exists(client_secrets_path):
        print(f"\n❌ Файл {client_secrets_path} не найден!")
        print("\nИнструкция:")
        print("1. Откройте https://console.cloud.google.com")
        print("2. APIs & Services → Credentials")
        print("3. Create Credentials → OAuth client ID")
        print("4. Application type: Desktop app")
        print("5. Скачайте JSON и сохраните как client_secrets.json")
        return
    
    creds = get_oauth_credentials(client_secrets_path)
    
    if creds:
        print("\n✅ Авторизация успешна!")
        print("Токен сохранён в token.json")
        print("Теперь можно запускать приложение")
    else:
        print("\n❌ Авторизация не удалась")


if __name__ == '__main__':
    run_oauth_flow()
