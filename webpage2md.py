import argparse
import json
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright


NOISE_SELECTORS = [
    "script",
    "style",
    "noscript",
    "svg",
    "iframe",
]
MAIN_TEXT_TAGS = ["p", "li", "dt", "dd", "blockquote", "pre", "td", "th"]
MAX_TEXT_ITEMS = 300
MAX_LINKS = 300
MAX_TABLES = 50
MAX_SELECT_OPTIONS = 100


class Webpage2MD:
    def __init__(self, url: str):
        self.url = url

    def get_html_document(self) -> str:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(self.url, wait_until="load", timeout=60_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=5_000)
                except Exception:
                    pass
                return page.content()
            finally:
                page.close()
                browser.close()

    def extract_page_features(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        self._prune_noise(soup)

        root = soup.find("main") or soup.body or soup
        title = self._normalize_text(soup.title.get_text(" ", strip=True)) if soup.title else ""
        meta_description = self._extract_meta_description(soup)
        headings = self._extract_headings(root)
        heading_texts = [item["text"] for item in headings]
        heading_text = "\n".join(heading_texts)
        text_blocks = self._extract_text_blocks(root)
        links = self._extract_links(root)
        tables = self._extract_tables(root)
        selects = self._extract_select_elements(root)
        forms = self._extract_form_elements(root)

        body_text = "\n".join(block["text"] for block in text_blocks)
        page_type_hints = self._infer_page_type_hints(
            title=title,
            headings=headings,
            body_text=body_text,
            tables=tables,
            links=links,
            selects=selects,
        )

        return {
            "url": self.url,
            "title": title,
            "meta_description": meta_description,
            "headings": headings,
            "heading_texts": heading_texts,
            "heading_text": heading_text,
            "body_text": body_text,
            "text_blocks": text_blocks,
            "tables": tables,
            "links": links,
            "select_elements": selects,
            "forms": forms,
            "page_type_hints": page_type_hints,
        }

    def run(self) -> dict:
        html = self.get_html_document()
        return self.extract_page_features(html)

    def _prune_noise(self, soup: BeautifulSoup) -> None:
        for selector in NOISE_SELECTORS:
            for tag in soup.select(selector):
                tag.decompose()

    def _extract_meta_description(self, soup: BeautifulSoup) -> str:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if not meta:
            return ""
        return self._normalize_text(meta.get("content", ""))

    def _extract_headings(self, root: Tag) -> list[dict]:
        headings = []
        for tag in root.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            text = self._normalize_text(tag.get_text(" ", strip=True))
            if not text:
                continue
            headings.append(
                {
                    "level": tag.name,
                    "text": text,
                }
            )
        return headings

    def _extract_text_blocks(self, root: Tag) -> list[dict]:
        blocks = []
        for tag in root.find_all(MAIN_TEXT_TAGS):
            text = self._normalize_text(tag.get_text(" ", strip=True))
            if not text:
                continue
            blocks.append(
                {
                    "tag": tag.name,
                    "text": text,
                }
            )
            if len(blocks) >= MAX_TEXT_ITEMS:
                break
        return blocks

    def _extract_links(self, root: Tag) -> list[dict]:
        links = []
        for tag in root.find_all("a", href=True):
            href = self._normalize_text(tag.get("href", ""))
            text = self._normalize_text(tag.get_text(" ", strip=True))
            if not href:
                continue
            links.append(
                {
                    "text": text,
                    "href": href,
                    "absolute_href": urljoin(self.url, href),
                }
            )
            if len(links) >= MAX_LINKS:
                break
        return links

    def _extract_tables(self, root: Tag) -> list[dict]:
        tables = []
        for table in root.find_all("table"):
            rows = []
            header = []
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                cell_values = [self._normalize_text(cell.get_text(" ", strip=True)) for cell in cells]
                if not any(cell_values):
                    continue
                if not header and row.find("th"):
                    header = cell_values
                rows.append(cell_values)

            if not rows:
                continue

            caption_tag = table.find("caption")
            tables.append(
                {
                    "caption": self._normalize_text(caption_tag.get_text(" ", strip=True)) if caption_tag else "",
                    "headers": header,
                    "rows": rows,
                }
            )
            if len(tables) >= MAX_TABLES:
                break
        return tables

    def _extract_select_elements(self, root: Tag) -> list[dict]:
        selects = []
        for select in root.find_all("select"):
            options = []
            for option in select.find_all("option"):
                options.append(
                    {
                        "value": option.get("value", ""),
                        "text": self._normalize_text(option.get_text(" ", strip=True)),
                        "selected": option.has_attr("selected"),
                    }
                )
                if len(options) >= MAX_SELECT_OPTIONS:
                    break

            selects.append(
                {
                    "name": select.get("name", ""),
                    "id": select.get("id", ""),
                    "options": options,
                }
            )
        return selects

    def _extract_form_elements(self, root: Tag) -> list[dict]:
        forms = []
        for form in root.find_all("form"):
            inputs = []
            for field in form.find_all(["input", "textarea", "button"]):
                inputs.append(
                    {
                        "tag": field.name,
                        "type": field.get("type", ""),
                        "name": field.get("name", ""),
                        "id": field.get("id", ""),
                        "value": field.get("value", ""),
                    }
                )

            forms.append(
                {
                    "action": form.get("action", ""),
                    "method": form.get("method", "get").lower(),
                    "inputs": inputs,
                }
            )
        return forms

    def _infer_page_type_hints(
        self,
        title: str,
        headings: list[dict],
        body_text: str,
        tables: list[dict],
        links: list[dict],
        selects: list[dict],
    ) -> list[str]:
        hints = []
        combined_text = " ".join(
            [
                title,
                " ".join(item["text"] for item in headings),
                body_text[:4000],
            ]
        ).lower()

        keyword_map = {
            "faq": ["faq", "よくあるご質問", "質問", "q&a"],
            "procedure": ["手続き", "申込", "申し込み", "契約", "解約", "利用開始", "お引越し"],
            "news": ["お知らせ", "ニュース", "news", "報道発表"],
            "blog": ["ブログ", "コラム", "記事", "読み物"],
            "event": ["イベント", "セミナー", "開催", "参加"],
            "link_collection": ["リンク集", "関連リンク", "一覧"],
            "product_service": ["サービス", "料金", "プラン", "機能", "特長"],
        }

        for hint, keywords in keyword_map.items():
            if any(keyword.lower() in combined_text for keyword in keywords):
                hints.append(hint)

        if tables:
            hints.append("has_table")
        if selects:
            hints.append("has_select")
        if len(links) >= 20:
            hints.append("many_links")

        return sorted(set(hints))

    def _normalize_text(self, value: str) -> str:
        return " ".join(value.split())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch a webpage and extract features for page classification.")
    parser.add_argument("url", help="Target page URL.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON output file path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    extractor = Webpage2MD(args.url)
    result = extractor.run()

    output = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
        print(f"saved={args.output}")
        return

    print(output)


if __name__ == "__main__":
    main()
