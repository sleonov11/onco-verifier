import requests
import json
import uuid
import time
import urllib3
from typing import Optional
from .config import GIGACHAT_API_KEY, logger
#откл. предупреждение ssl для демо
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GigaChatClient:
    def __init__(self):
        self.api_key = GIGACHAT_API_KEY
        self.token: Optional[str] = None
        self.token_expires: float = 0

    def _get_token(self) -> str:
        if self.token and time.time() < self.token_expires - 60:
            return self.token

        response = requests.post(
            "https://ngw.devices.sberbank.ru:9443/api/v2/oauth",
            headers={
                "Authorization": f"Basic {self.api_key}",
                "RqUID": str(uuid.uuid4()),
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={"scope": "GIGACHAT_API_PERS"},
            verify=False,
            timeout=10
        )
        response.raise_for_status()

        data = response.json()
        self.token = data["access_token"]
        self.token_expires = time.time() + 1700

        logger.info("GigaChat token obtained")
        return self.token

    def generate(self, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> str:
        token = self._get_token()

        response = requests.post(
            "https://gigachat.devices.sberbank.ru/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={
                "model": "GigaChat-Pro",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": temperature,
                "max_tokens": 2000
            },
            verify=False,
            timeout=30
        )
        response.raise_for_status()

        return response.json()["choices"][0]["message"]["content"]


_gigachat: Optional[GigaChatClient] = None


def get_gigachat() -> GigaChatClient:
    global _gigachat
    if _gigachat is None:
        _gigachat = GigaChatClient()
    return _gigachat