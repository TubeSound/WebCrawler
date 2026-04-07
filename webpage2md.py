import os
import argparse
import json
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from markdownify import markdownify as md
from playwright.sync_api import sync_playwright


HEADING_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6"]
DECORATIVE_IMAGE_PATTERNS = [
    "/img/common/icon_",
    "/img/support/icon_",
]
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
        markdown_blocks = self._extract_markdown_blocks(root)
        markdown_text = "\n\n".join(block["markdown"] for block in markdown_blocks)
        links = self._extract_links(root)
        tables = self._extract_tables(root)
        selects = self._extract_select_elements(root)
        forms = self._extract_form_elements(root)

        body_text = self._normalize_text(root.get_text("\n", strip=True))
        heading_texts = self._extract_heading_texts_from_blocks(markdown_blocks)
        page_type_hints = self._infer_page_type_hints(
            title=title,
            heading_texts=heading_texts,
            body_text=body_text,
            tables=tables,
            links=links,
            selects=selects,
        )

        return {
            "url": self.url,
            "title": title,
            "meta": meta,
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

    def build_markdown_document(self, result: dict) -> str:
        markdown_blocks = result.get("markdown_blocks", [])
        if not markdown_blocks:
            return result.get("markdown_text", "").strip()

        parts = []
        for block in markdown_blocks:
            markdown = (block.get("markdown") or "").strip()
            if not markdown:
                continue
            parts.append(markdown)
        return "\n\n---\n\n".join(parts)

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

        for index, tag in enumerate(root.find_all(["section"], recursive=True), start=1):
            if not isinstance(tag, Tag):
                continue
            if tag.find_parent("section") is not None:
                continue
            tag_for_markdown = self._prepare_tag_for_markdown(tag)

            text = self._normalize_text(tag_for_markdown.get_text(" ", strip=True))
            if not text:
                continue

            heading = self._find_first_heading(tag) or self._find_table_heading(tag)
            heading_tags = self._extract_section_heading_tags(tag)
            html = str(tag_for_markdown)
            if html in seen_ids:
                continue
            seen_ids.add(html)

            markdown = self._tag_to_markdown_html(html).strip()
            if not markdown:
                continue

            sections.append(
                {
                    "section_index": index,
                    "heading": heading or "section",
                    "tag": "section",
                    "heading_tags": heading_tags,
                    "markdown": markdown,
                }
            )

        return sections

    # マークダウンブロックを追加する。
    def _append_markdown_block(self, blocks: list[dict], heading: str, nodes: list[Tag]) -> None:
        if not nodes:
            return
        prepared_nodes = [self._prepare_tag_for_markdown(node) for node in nodes]
        html = "\n".join(str(node) for node in prepared_nodes)
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
        heading_texts: list[str],
        body_text: str,
        tables: list[dict],
        links: list[dict],
        selects: list[dict],
    ) -> list[str]:
        hints = []
        combined_text = " ".join(
            [
                title,
                " ".join(heading_texts),
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

    def _extract_heading_texts_from_blocks(self, markdown_blocks: list[dict]) -> list[str]:
        heading_texts = []
        for block in markdown_blocks:
            for heading in block.get("heading_tags", []):
                text = self._normalize_text(heading.get("text", ""))
                if text:
                    heading_texts.append(text)
        return heading_texts

    def _find_first_heading(self, root: Tag) -> str:
        heading = root.find(HEADING_TAGS)
        if not heading:
            return ""
        return self._normalize_text(heading.get_text(" ", strip=True))

    def _extract_section_heading_tags(self, section: Tag) -> list[dict]:
        heading_tags = []
        for heading in section.find_all(HEADING_TAGS):
            text = self._normalize_text(heading.get_text(" ", strip=True))
            if not text:
                continue
            heading_tags.append(
                {
                    "tag": heading.name,
                    "text": text,
                }
            )
        return heading_tags

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
        return self._cleanup_markdown(markdown)

    def _prepare_tag_for_markdown(self, tag: Tag) -> BeautifulSoup:
        cloned = BeautifulSoup(str(tag), "html.parser")
        for table in cloned.find_all("table"):
            table.decompose()
        self._remove_redundant_images(cloned)
        self._remove_duplicate_text_nodes(cloned)
        return cloned

    def _remove_redundant_images(self, soup: BeautifulSoup) -> None:
        seen_image_keys = set()
        for image in soup.find_all("img"):
            src = image.get("src", "") or ""
            alt = self._normalize_text(image.get("alt", ""))
            src_lower = src.lower()

            if any(pattern in src_lower for pattern in DECORATIVE_IMAGE_PATTERNS):
                image.decompose()
                continue

            normalized_src = (
                src_lower
                .replace("_pc.", ".")
                .replace("_sp.", ".")
                .replace("-pc.", ".")
                .replace("-sp.", ".")
            )
            image_key = (normalized_src, alt)
            if image_key in seen_image_keys:
                image.decompose()
                continue

            seen_image_keys.add(image_key)

    def _remove_duplicate_text_nodes(self, soup: BeautifulSoup) -> None:
        candidate_tags = ["p", "li", "dt", "dd", "span", "div"]
        for parent in soup.find_all(True):
            seen_texts = set()
            for child in list(parent.find_all(candidate_tags, recursive=False)):
                text = self._normalize_text(child.get_text(" ", strip=True))
                if not text:
                    continue
                text_key = (child.name, text)
                if text_key in seen_texts:
                    child.decompose()
                    continue
                seen_texts.add(text_key)

    def _cleanup_markdown(self, markdown: str) -> str:
        blocks = []
        current_lines = []

        for line in markdown.splitlines():
            if line.strip():
                current_lines.append(line.rstrip())
                continue
            if current_lines:
                blocks.append("\n".join(current_lines).strip())
                current_lines = []
        if current_lines:
            blocks.append("\n".join(current_lines).strip())

        cleaned_blocks = []
        previous_block_key = ""
        for block in blocks:
            block_key = self._normalize_markdown_block(block)
            if not block_key:
                continue
            if block_key == previous_block_key:
                continue
            cleaned_blocks.append(block)
            previous_block_key = block_key

        return "\n\n".join(cleaned_blocks).strip()

    def _normalize_markdown_block(self, block: str) -> str:
        normalized = block.replace("  \n", "\n")
        normalized = normalized.replace("\\\n", "\n")
        normalized = normalized.replace("\n", " ")
        normalized = self._normalize_text(normalized)
        return normalized

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
    parser.add_argument(
        "--markdown",
        type=Path,
        help="Markdown output file path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    extractor = Webpage2markdown(args.url)
    result = extractor.run()
    markdown_document = extractor.build_markdown_document(result)

    json_output = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        path = OUTPUT_DIR / Path(args.output)
        path.write_text(json_output, encoding="utf-8")
        print(f"saved={args.output}")
    else:
        print(json_output)

    if args.markdown:
        markdown_path = OUTPUT_DIR / Path(args.markdown)
        markdown_path.write_text(markdown_document, encoding="utf-8")
        print(f"saved={args.markdown}")
    elif not args.output:
        print("\n---\n")
        print(markdown_document)


if __name__ == "__main__":
    main()
