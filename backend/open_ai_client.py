import os
from typing import Any, Dict, Generator
from dotenv import load_dotenv
import requests
import json

load_dotenv()


class OpenAIHTTPClient:
    def __init__(self, *, model: str) -> None:
        self.model = model
        self.timeout_s: int = 30
        self.api_key = os.environ["OPENAI_API_KEY"]
        self.base_url = os.environ["OPENAI_BASE_URL"].rstrip("/")

        if not self.api_key:
            raise RuntimeError("Missing OPENAI_API_KEY")
        self.last_usage: dict = {}


    def draft_plan_json(self, *, user_query: str, schema_hint: str) -> Dict[str, Any]:
        system = (
            "You are a query-planning compiler working for the architectural office OMA.\n"
            "Output ONLY a single valid JSON object that matches the schema hint.\n"
            "No markdown. No comments. No extra keys.\n"
        )

        user = f"""User query:
{user_query}

Schema and constraints:
{schema_hint}

Return ONLY the JSON object.
"""

        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "temperature": 0.0,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        self.last_usage = data.get("usage", {})
        choices = data.get("choices", [])
        if not choices:
            raise RuntimeError(f"No choices returned: {data}")
        content = choices[0]["message"]["content"]
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Model returned invalid JSON:\n{content}") from e


    def chat(self, *, system: str, user: str) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        self.last_usage = data.get("usage", {})
        choices = data.get("choices", [])
        if not choices:
            raise RuntimeError(f"No choices returned: {data}")
        return choices[0]["message"]["content"]


    def chat_stream(self, *, system: str, user: str) -> Generator[str, None, None]:
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        self.last_usage = {}
        payload = {
            "model": self.model,
            "stream": True,
            "stream_options": {"include_usage": True},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        with requests.post(url, headers=headers, json=payload, timeout=self.timeout_s, stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                data = line[6:]
                if data == "[DONE]":
                    break
                chunk = json.loads(data)
                if chunk.get("usage"):
                    self.last_usage = chunk["usage"]
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                delta = choices[0].get("delta", {})
                token = delta.get("content")
                if token:
                    yield token


    def embed_text(self, *, text: str) -> list[float]:
        if not isinstance(text, str) or not text.strip():
            return []

        url = f"{self.base_url}/embeddings"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "input": text,
            "encoding_format": "float",
        }

        r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        items = data.get("data", [])
        if not items:
            return []
        embedding = items[0].get("embedding")
        if isinstance(embedding, list):
            return embedding
        return []
