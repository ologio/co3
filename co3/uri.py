from urllib import parse


class URI:
    def __init__(self, url_str: str):
        self.url_str = url_str

class URL(URI):
    def __init__(self, url_str: str):
        self.url_str = url_str

class URN(URI):
    def __init__(self, url_str: str):
        self.url_str = url_str

