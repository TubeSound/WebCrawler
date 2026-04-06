import json
from pathlib import Path
from urllib.parse import urldefrag, urlparse, urlunparse


LinkItem = dict[str, str]
LinksByDomain = dict[str, list[LinkItem]]


# URLを正規化して重複判定しやすい形にそろえる。
def normalize_url(url: str) -> str:
    url, _ = urldefrag(url)
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    normalized_parts = (
        parsed.scheme.lower(),
        netloc,
        path,
        parsed.params,
        parsed.query,
        "",
    )
    return urlunparse(normalized_parts)


# URLから分類用のドメイン名を取り出す。
def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


# URL一覧をドメイン別の辞書にまとめる。
def group_links_by_domain(link_items: list[LinkItem]) -> LinksByDomain:
    grouped_links: LinksByDomain = {}
    for item in link_items:
        domain = get_domain(item["url"])
        grouped_links.setdefault(domain, []).append(item)
    return dict(sorted(grouped_links.items()))


# ドメイン別辞書の総件数を数える。
def count_links(links_by_domain: LinksByDomain) -> int:
    return sum(len(items) for items in links_by_domain.values())


# 取得したドメイン・タイトル・URLをJSON Lines形式で書き出す。
def write_links_to_jsonl(links_by_domain: LinksByDomain, output_file: Path) -> None:
    with output_file.open("w", encoding="utf-8") as jsonl_file:
        for domain, items in links_by_domain.items():
            for item in items:
                record = {
                    "domain": domain,
                    "title": item["title"],
                    "url": item["url"],
                }
                jsonl_file.write(json.dumps(record, ensure_ascii=False) + "\n")

