import argparse
import json
import os
import re
from collections import deque
from pathlib import Path
from urllib.parse import parse_qs, urldefrag, urljoin, urlparse, urlunparse

from playwright.sync_api import sync_playwright

from libs_crawl import count_links, get_domain, group_links_by_domain, write_links_to_jsonl


OUTPUT_DIR = Path("./crawl_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

DETAIL_URL_PATTERN = re.compile(r"/faq/detail")
SEARCH_URL_PATTERN = re.compile(r"/faq/search")
FAQ_ID_PATTERN = re.compile(r"(?:[?&]faqId=)(\d+)")
CATEGORY_ID_PATTERN = re.compile(r"(?:[?&]categoryId=)(\d+)")


class CrawlerSearch:
    def __init__(
        self,
        base_url: str,
        category_ids: list[str | int],
        allowed_domains: list[str],
        output_file: Path,
        category_url_template: str,
        detail_url_templates: list[str],
        wait_ms: int = 5000,
        max_clicks_per_category: int = 20,
    ) -> None:
        self.base_url = base_url
        self.category_ids = [str(category_id).strip() for category_id in category_ids if str(category_id).strip()]
        self.allowed_domains = {self._normalize_allowed_domain(domain) for domain in allowed_domains if domain.strip()}
        self.output_file = output_file
        self.category_url_template = category_url_template
        self.detail_url_templates = detail_url_templates
        self.wait_ms = wait_ms
        self.max_clicks_per_category = max_clicks_per_category
        self.discovered_category_ids: set[str] = set()

    def _normalize_allowed_domain(self, value: str) -> str:
        parsed = urlparse(value if "://" in value else f"https://{value}")
        domain = (parsed.netloc or parsed.path).strip().strip("/").lower()
        return domain.removeprefix("www.")

    def _is_allowed_link(self, url: str) -> bool:
        if not self.allowed_domains:
            return True
        return get_domain(url).lower().removeprefix("www.") in self.allowed_domains

    def _normalize_url(self, url: str) -> str:
        url, _ = urldefrag(url)
        parsed = urlparse(url)
        normalized_parts = (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            parsed.params,
            parsed.query,
            "",
        )
        return urlunparse(normalized_parts)

    def _new_context(self, browser):
        return browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            viewport={"width": 1440, "height": 900},
            extra_http_headers={
                "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )

    def _category_url(self, category_id: str) -> str:
        return self._normalize_url(self.category_url_template.format(categoryId=category_id))

    def _extract_category_ids_from_text(self, text: str) -> set[str]:
        category_ids = set(CATEGORY_ID_PATTERN.findall(text))
        category_ids.update(re.findall(r"""categoryId["'\s:=]+["']?(\d+)""", text))
        return {category_id for category_id in category_ids if category_id}

    def _extract_ids_from_url(self, url: str) -> tuple[str, str]:
        normalized_url = self._normalize_url(url)
        parsed = urlparse(normalized_url)
        query = parse_qs(parsed.query)
        faq_id = (query.get("faqId") or [""])[0]
        category_id = (query.get("categoryId") or [""])[0]

        if not faq_id:
            faq_match = FAQ_ID_PATTERN.search(normalized_url)
            if faq_match:
                faq_id = faq_match.group(1)
        if not category_id:
            category_match = CATEGORY_ID_PATTERN.search(normalized_url)
            if category_match:
                category_id = category_match.group(1)

        return category_id, faq_id

    def _build_detail_urls(self, category_id: str, faq_id: str) -> list[str]:
        urls = []
        for template in self.detail_url_templates:
            try:
                url = self._normalize_url(template.format(categoryId=category_id, faqId=faq_id))
            except KeyError:
                continue
            urls.append(url)
        return urls

    def _wait_for_search_results(self, page) -> None:
        selectors = [
            "a[href*='/faq/detail'][href*='faqId=']",
            "a[href*='/faq/search'][href*='categoryId=']",
        ]
        for selector in selectors:
            try:
                page.wait_for_selector(selector, timeout=self.wait_ms)
                page.wait_for_timeout(1500)
                return
            except Exception:
                continue
        page.wait_for_timeout(self.wait_ms)

    def _response_category_handler(self, response) -> None:
        self.discovered_category_ids.update(self._extract_category_ids_from_text(response.url))
        try:
            body = response.text()
        except Exception:
            return
        self.discovered_category_ids.update(self._extract_category_ids_from_text(body))

    def _collect_category_ids_from_page(self, page) -> set[str]:
        category_ids = set()
        category_ids.update(self._extract_category_ids_from_text(page.content()))

        for frame in page.frames:
            try:
                category_ids.update(self._extract_category_ids_from_text(frame.url))
                category_ids.update(self._extract_category_ids_from_text(frame.content()))
            except Exception:
                continue

        selectors = ["a[href]", "[data-href]", "[data-url]", "[onclick]"]
        for selector in selectors:
            try:
                elements = page.query_selector_all(selector)
            except Exception:
                elements = []
            for element in elements:
                for attribute in ["href", "data-href", "data-url", "onclick"]:
                    try:
                        value = element.get_attribute(attribute)
                    except Exception:
                        value = None
                    if value:
                        category_ids.update(self._extract_category_ids_from_text(value))
        return category_ids

    def _expand_category_controls(self, page) -> None:
        selectors = [
            "button",
            "[role='button']",
            "summary",
            "[aria-expanded='false']",
        ]
        clicked = 0
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = locator.count()
            except Exception:
                continue
            for index in range(count):
                if clicked >= self.max_clicks_per_category:
                    return
                candidate = locator.nth(index)
                try:
                    if not candidate.is_visible() or not candidate.is_enabled():
                        continue
                    candidate.scroll_into_view_if_needed(timeout=2000)
                    candidate.click(timeout=3000)
                    clicked += 1
                except Exception:
                    continue
        if clicked:
            try:
                page.wait_for_load_state("networkidle", timeout=self.wait_ms)
            except Exception:
                page.wait_for_timeout(1500)

    def discover_category_ids(self) -> list[str]:
        self.discovered_category_ids = set()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = self._new_context(browser)
            page = context.new_page()
            page.on("response", self._response_category_handler)
            try:
                page.goto(self.base_url, wait_until="load", timeout=60000)
                try:
                    page.wait_for_load_state("networkidle", timeout=self.wait_ms)
                except Exception:
                    page.wait_for_timeout(self.wait_ms)

                self.discovered_category_ids.update(self._collect_category_ids_from_page(page))
                self._expand_category_controls(page)
                self.discovered_category_ids.update(self._collect_category_ids_from_page(page))
            finally:
                page.close()
                context.close()
                browser.close()
        return sorted(self.discovered_category_ids, key=int)

    def _collect_detail_links_from_page(self, page, fallback_category_id: str) -> dict[str, str]:
        link_map: dict[str, str] = {}
        anchors = page.query_selector_all("a[href]")
        for anchor in anchors:
            href = anchor.get_attribute("href")
            if not href:
                continue
            absolute_url = self._normalize_url(urljoin(page.url or self.base_url, href))
            if not self._is_allowed_link(absolute_url):
                continue
            if not DETAIL_URL_PATTERN.search(absolute_url):
                continue

            category_id, faq_id = self._extract_ids_from_url(absolute_url)
            if not faq_id:
                continue
            category_id = category_id or fallback_category_id
            title = " ".join(((anchor.text_content() or "").split()))
            for detail_url in self._build_detail_urls(category_id, faq_id):
                link_map.setdefault(detail_url, title or detail_url)
        return link_map

    def _collect_search_page_links(self, page, fallback_category_id: str) -> list[str]:
        page_links: list[str] = []
        seen: set[str] = set()
        anchors = page.query_selector_all("a[href]")

        for anchor in anchors:
            href = anchor.get_attribute("href")
            if not href:
                continue
            absolute_url = self._normalize_url(urljoin(page.url or self.base_url, href))
            if not self._is_allowed_link(absolute_url):
                continue
            if not SEARCH_URL_PATTERN.search(absolute_url):
                continue

            category_id, _ = self._extract_ids_from_url(absolute_url)
            if category_id != fallback_category_id:
                continue
            if absolute_url in seen:
                continue
            seen.add(absolute_url)
            page_links.append(absolute_url)

        return page_links

    def _collect_faq_ids_from_page(self, page, fallback_category_id: str) -> set[str]:
        faq_ids: set[str] = set()
        anchors = page.query_selector_all("a[href]")
        for anchor in anchors:
            href = anchor.get_attribute("href")
            if not href:
                continue
            absolute_url = self._normalize_url(urljoin(page.url or self.base_url, href))
            if not self._is_allowed_link(absolute_url):
                continue
            if not DETAIL_URL_PATTERN.search(absolute_url):
                continue

            category_id, faq_id = self._extract_ids_from_url(absolute_url)
            if not faq_id:
                continue
            if (category_id and category_id == fallback_category_id) or not category_id:
                faq_ids.add(faq_id)

        html = page.content()
        for faq_id in FAQ_ID_PATTERN.findall(html):
            faq_ids.add(faq_id)

        return faq_ids

    def _collect_category_detail_links(self, page, category_id: str) -> dict[str, str]:
        category_url = self._category_url(category_id)
        pending_pages = deque([category_url])
        queued_pages = {category_url}
        visited_pages: set[str] = set()
        links: dict[str, str] = {}

        while pending_pages and len(visited_pages) < self.max_clicks_per_category:
            current_url = pending_pages.popleft()
            if current_url in visited_pages:
                continue

            page.goto(current_url, wait_until="load", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=self.wait_ms)
            except Exception:
                page.wait_for_timeout(self.wait_ms)
            self._wait_for_search_results(page)

            visited_pages.add(current_url)

            for detail_url, title in self._collect_detail_links_from_page(page, category_id).items():
                links.setdefault(detail_url, title)

            next_clicks = 0
            while next_clicks < self.max_clicks_per_category:
                next_button = page.locator("button:has-text('次の10件'), a:has-text('次の10件')").first
                try:
                    if next_button.count() == 0 or not next_button.is_visible() or not next_button.is_enabled():
                        break
                    next_button.scroll_into_view_if_needed(timeout=3000)
                    next_button.click(timeout=5000)
                    self._wait_for_search_results(page)
                    next_clicks += 1
                except Exception:
                    break

                for detail_url, title in self._collect_detail_links_from_page(page, category_id).items():
                    links.setdefault(detail_url, title)

            for next_page_url in self._collect_search_page_links(page, category_id):
                if next_page_url in visited_pages or next_page_url in queued_pages:
                    continue
                queued_pages.add(next_page_url)
                pending_pages.append(next_page_url)

        return links

    def _collect_category_faq_ids(self, page, category_id: str) -> set[str]:
        category_url = self._category_url(category_id)
        pending_pages = deque([category_url])
        queued_pages = {category_url}
        visited_pages: set[str] = set()
        faq_ids: set[str] = set()

        while pending_pages and len(visited_pages) < self.max_clicks_per_category:
            current_url = pending_pages.popleft()
            if current_url in visited_pages:
                continue

            page.goto(current_url, wait_until="load", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=self.wait_ms)
            except Exception:
                page.wait_for_timeout(self.wait_ms)
            self._wait_for_search_results(page)

            visited_pages.add(current_url)
            faq_ids.update(self._collect_faq_ids_from_page(page, category_id))

            next_clicks = 0
            while next_clicks < self.max_clicks_per_category:
                next_button = page.locator("button:has-text('次の10件'), a:has-text('次の10件')").first
                try:
                    if next_button.count() == 0 or not next_button.is_visible() or not next_button.is_enabled():
                        break
                    next_button.scroll_into_view_if_needed(timeout=3000)
                    next_button.click(timeout=5000)
                    self._wait_for_search_results(page)
                    next_clicks += 1
                except Exception:
                    break

                faq_ids.update(self._collect_faq_ids_from_page(page, category_id))

            for next_page_url in self._collect_search_page_links(page, category_id):
                if next_page_url in visited_pages or next_page_url in queued_pages:
                    continue
                queued_pages.add(next_page_url)
                pending_pages.append(next_page_url)

        return faq_ids

    def _click_more_buttons(self, page) -> None:
        selectors = [
            "button:has-text('もっと見る')",
            "button:has-text('さらに表示')",
            "button:has-text('次へ')",
            "a:has-text('もっと見る')",
            "a:has-text('さらに表示')",
            "a:has-text('次へ')",
        ]
        clicked = 0
        while clicked < self.max_clicks_per_category:
            clicked_this_round = False
            for selector in selectors:
                locator = page.locator(selector).first
                try:
                    if locator.count() == 0:
                        continue
                    if not locator.is_visible() or not locator.is_enabled():
                        continue
                    locator.scroll_into_view_if_needed(timeout=3000)
                    locator.click(timeout=5000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=self.wait_ms)
                    except Exception:
                        page.wait_for_timeout(1500)
                    clicked += 1
                    clicked_this_round = True
                    break
                except Exception:
                    continue
            if not clicked_this_round:
                break

    def crawl(self) -> tuple[dict[str, list[dict[str, str]]], int]:
        if not self.category_ids:
            raise ValueError("category_ids must not be empty.")

        links: dict[str, str] = {}
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = self._new_context(browser)
            page = context.new_page()
            try:
                for category_id in self.category_ids:
                    for detail_url, title in self._collect_category_detail_links(page, category_id).items():
                        links.setdefault(detail_url, title)
            finally:
                page.close()
                context.close()
                browser.close()

        link_items = [{"url": url, "title": title} for url, title in sorted(links.items())]
        grouped = group_links_by_domain(link_items)
        total_count = count_links(grouped)
        write_links_to_jsonl(grouped, OUTPUT_DIR / self.output_file)
        return grouped, total_count

    def discover_faq_ids(self) -> dict[str, list[str]]:
        if not self.category_ids:
            return {}

        faq_ids_by_category: dict[str, set[str]] = {category_id: set() for category_id in self.category_ids}
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = self._new_context(browser)
            page = context.new_page()
            try:
                for category_id in self.category_ids:
                    faq_ids_by_category[category_id].update(self._collect_category_faq_ids(page, category_id))
            finally:
                page.close()
                context.close()
                browser.close()

        return {
            category_id: sorted(faq_ids, key=int)
            for category_id, faq_ids in faq_ids_by_category.items()
        }


def load_config(config_file: Path) -> dict:
    with config_file.open("r", encoding="utf-8") as config_input:
        config = json.load(config_input)

    base_url = config.get("base_url")
    category_ids = config.get("category_ids")
    allowed_domains = config.get("allowed_domains", [])
    output_file = config.get("output_file")
    category_url_template = config.get("category_url_template")
    detail_url_templates = config.get("detail_url_templates", [])
    wait_ms = config.get("wait_ms", 5000)
    max_clicks_per_category = config.get("max_clicks_per_category", 20)

    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty string.")
    if not isinstance(category_ids, list) or not all(str(category_id).strip() for category_id in category_ids):
        raise ValueError("category_ids must be a list.")
    if not isinstance(allowed_domains, list) or not all(isinstance(domain, str) and domain.strip() for domain in allowed_domains):
        raise ValueError("allowed_domains must be a non-empty list of strings.")
    if not isinstance(output_file, str) or not output_file.strip():
        raise ValueError("output_file must be a non-empty string.")
    if not isinstance(category_url_template, str) or not category_url_template.strip():
        raise ValueError("category_url_template must be a non-empty string.")
    if not isinstance(detail_url_templates, list) or not all(isinstance(template, str) and template.strip() for template in detail_url_templates):
        raise ValueError("detail_url_templates must be a list of strings.")
    if not isinstance(wait_ms, int) or wait_ms <= 0:
        raise ValueError("wait_ms must be a positive integer.")
    if not isinstance(max_clicks_per_category, int) or max_clicks_per_category <= 0:
        raise ValueError("max_clicks_per_category must be a positive integer.")

    return {
        "base_url": base_url,
        "category_ids": category_ids,
        "allowed_domains": allowed_domains,
        "output_file": Path(output_file),
        "category_url_template": category_url_template,
        "detail_url_templates": detail_url_templates,
        "wait_ms": wait_ms,
        "max_clicks_per_category": max_clicks_per_category,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect FAQ detail URLs from category search pages.")
    parser.add_argument("--config", type=Path, required=True, help="Crawler config JSON file path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    crawler = CrawlerSearch(
        base_url=config["base_url"],
        category_ids=config["category_ids"],
        allowed_domains=config["allowed_domains"],
        output_file=config["output_file"],
        category_url_template=config["category_url_template"],
        detail_url_templates=config["detail_url_templates"],
        wait_ms=config["wait_ms"],
        max_clicks_per_category=config["max_clicks_per_category"],
    )
    discovered_category_ids = crawler.discover_category_ids()
    print(f"[category_ids] count={len(discovered_category_ids)}")
    for category_id in discovered_category_ids:
        print(category_id)

    if not crawler.category_ids:
        return

    faq_ids_by_category = crawler.discover_faq_ids()
    for category_id, faq_ids in faq_ids_by_category.items():
        print(f"[faq_ids] category_id={category_id} count={len(faq_ids)}")
        for faq_id in faq_ids:
            print(faq_id)

    grouped, total_count = crawler.crawl()

    print(f"count={total_count}")
    print(f"saved={config['output_file']}")
    for domain, items in grouped.items():
        print(f"[{domain}] count={len(items)}")
        for item in items:
            print(f"{item['url']} | {item['title']}")


if __name__ == "__main__":
    main()
