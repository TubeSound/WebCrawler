import os
import argparse
import json
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from markdownify import markdownify as md
from playwright.sync_api import sync_playwright


HEADING_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6"]
NOISE_SELECTORS = [
    "script",
    "style",
    "noscript",
    "svg",
    "iframe",
]
MAIN_TEXT_TAGS = ["p", "li", "dt", "dd", "blockquote", "pre", "td", "th"]
MARKDOWN_TAGS = ["p", "ul", "ol", "dl", "blockquote", "pre", "div", "section", "article"]
MAX_LINKS = 300
MAX_TABLES = 50
MAX_SELECT_OPTIONS = 100

OUTPUT_DIR = "./preprocess"
os.makedirs(OUTPUT_DIR, exist_ok=True)


class Webpage2markdown:
    def __init__(self, url: str):
        self.url = url

    # HTMLドキュメントを取得
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

    # ページ特徴量を抽出
    def extract_page_features(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        self._remove_noise(soup)

        root = soup.find("main") or soup.body or soup
        title = self._normalize_text(soup.title.get_text(" ", strip=True)) if soup.title else ""
        meta = self._extract_meta(soup)
        headings = self._extract_headings(root)
        heading_texts = [item["text"] for item in headings]
        heading_text = "\n".join(heading_texts)
        markdown_blocks = self._extract_markdown_blocks(root)
        markdown_text = "\n\n".join(block["markdown"] for block in markdown_blocks)
        links = self._extract_links(root)
        tables = self._extract_tables(root)
        selects = self._extract_select_elements(root)
        forms = self._extract_form_elements(root)

        body_text = self._normalize_text(root.get_text("\n", strip=True))
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
            "meta": meta,
            "headings": headings,
            "heading_texts": heading_texts,
            "heading_text": heading_text,
            "body_text": body_text,
            "markdown_blocks": markdown_blocks,
            "markdown_text": markdown_text,
            "tables": tables,
            "links": links,
            "select_elements": selects,
            "forms": forms,
            "page_type_hints": page_type_hints,
        }

    def run(self) -> dict:
        html = self.get_html_document()
        return self.extract_page_features(html)

    # ノイズを除去する
    def _remove_noise(self, soup: BeautifulSoup) -> None:
        for selector in NOISE_SELECTORS:
            for tag in soup.select(selector):
                tag.decompose()

    # メタデータを取得する
    def _extract_meta(self, soup: BeautifulSoup) -> str:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if not meta:
            return ""
        return self._normalize_text(meta.get("content", ""))

    # 見出しを取得する
    def _extract_headings(self, root: Tag) -> list[dict]:
        headings = []
        for tag in root.find_all(HEADING_TAGS):
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

    # マークダウンブロックを抽出する
    def _extract_markdown_blocks(self, root: Tag) -> list[dict]:
        section_blocks = self._extract_section_markdown_blocks(root)
        if section_blocks:
            return section_blocks

        blocks = []
        current_heading = self._find_first_heading(root) or "page-content"
        current_nodes: list[Tag] = []

        for node in self._iter_markdown_nodes(root):
            if node.name in HEADING_TAGS:
                self._append_markdown_block(blocks, current_heading, current_nodes)
                current_heading = self._normalize_text(node.get_text(" ", strip=True)) or current_heading
                current_nodes = [node]
                continue
            current_nodes.append(node)

        self._append_markdown_block(blocks, current_heading, current_nodes)
        return blocks

    # セクションを抽出してセクションごとにマークダウンを抽出
    def _extract_section_markdown_blocks(self, root: Tag) -> list[dict]:
        sections = []
        seen_ids = set()

        for tag in root.find_all(["section"], recursive=True):
            if not isinstance(tag, Tag):
                continue
            if tag.find("table"):
                tag_for_markdown = self._clone_without_tables(tag)
            else:
                tag_for_markdown = tag

            text = self._normalize_text(tag_for_markdown.get_text(" ", strip=True))
            if not text:
                continue

            heading = self._find_first_heading(tag) or self._find_table_heading(tag)
            html = str(tag_for_markdown)
            if html in seen_ids:
                continue
            seen_ids.add(html)

            markdown = self._tag_to_markdown_html(html).strip()
            if not markdown:
                continue

            sections.append(
                {
                    "heading": heading or "section",
                    "tag": "section",
                    "markdown": markdown,
                }
            )

        return sections

    # マークダウンブロックを追加する。
    def _append_markdown_block(self, blocks: list[dict], heading: str, nodes: list[Tag]) -> None:
        if not nodes:
            return
        html = "\n".join(str(node) for node in nodes)
        markdown = self._tag_to_markdown_html(html)
        markdown = markdown.strip()
        if not markdown:
            return
        blocks.append(
            {
                "heading": heading,
                "tag": "section",
                "markdown": markdown,
            }
        )

    def _iter_markdown_nodes(self, root: Tag):
        for child in root.children:
            if not isinstance(child, Tag):
                continue
            if child.name in HEADING_TAGS:
                yield child
                continue
            if child.name == "table":
                continue
            if child.name in MARKDOWN_TAGS:
                yield child
                continue
            yield from self._iter_markdown_nodes(child)

    # リンクを抽出する
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

    # テーブルを抽出する
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
            heading_context = self._find_table_heading(table)
            tables.append(
                {
                    "heading": heading_context,
                    "caption": self._normalize_text(caption_tag.get_text(" ", strip=True)) if caption_tag else "",
                    "headers": header,
                    "rows": rows,
                }
            )
            if len(tables) >= MAX_TABLES:
                break
        return tables

    # セレクト要素を抽出
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

    # HTML要素を抽出する
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

    def _find_first_heading(self, root: Tag) -> str:
        heading = root.find(HEADING_TAGS)
        if not heading:
            return ""
        return self._normalize_text(heading.get_text(" ", strip=True))

    def _find_table_heading(self, node: Tag) -> str:
        for previous in node.find_all_previous(HEADING_TAGS, limit=1):
            text = self._normalize_text(previous.get_text(" ", strip=True))
            if text:
                return text
        parent = node.parent
        while isinstance(parent, Tag):
            caption_like = parent.find(HEADING_TAGS)
            if caption_like:
                text = self._normalize_text(caption_like.get_text(" ", strip=True))
                if text:
                    return text
            parent = parent.parent
        return ""

    def _tag_to_markdown_html(self, html: str) -> str:
        markdown = md(
            html,
            heading_style="ATX",
            bullets="-",
            strip=["table"],
        ).strip()
        return markdown

    def _clone_without_tables(self, tag: Tag) -> BeautifulSoup:
        cloned = BeautifulSoup(str(tag), "html.parser")
        for table in cloned.find_all("table"):
            table.decompose()
        return cloned


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch a webpage and extract features for page classification.")
    parser.add_argument("url", help="Target page URL.")
    parser.add_argument(
        "--output",
        type=Path,
        help="JSON output file path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    extractor = Webpage2markdown(args.url)
    result = extractor.run()

    json_output = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        path = OUTPUT_DIR / Path(args.output)
        path.write_text(json_output, encoding="utf-8")
        print(f"saved={args.output}")
        return

    print(json_output)


if __name__ == "__main__":
    main()
