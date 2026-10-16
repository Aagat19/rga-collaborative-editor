"""Minimal line-oriented editor façade for the collaborative CRDT."""

from crdt_editor import Client


class Editor:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.cursor = 0

    def insert(self, text: str) -> None:
        self.client.insert_text(self.cursor, text)
        self.cursor += len(text)

    def backspace(self) -> None:
        if self.cursor:
            self.client.delete(self.cursor - 1)
            self.cursor -= 1

    def move(self, position: int) -> None:
        if not 0 <= position <= len(self.client.text()):
            raise IndexError("cursor out of range")
        self.cursor = position

    def render(self) -> str:
        return self.client.text()
