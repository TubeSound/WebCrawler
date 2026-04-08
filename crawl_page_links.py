import os
import asyncio
import argparse
from collections import Counter
import json
import logging
from pathlib import Path
from collections import deque
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright
from libs_crawl import count_links, get_domain, group_links_by_domain, normalize_url, write_links_to_jsonl

os.makedirs("./log/", exist_ok=True)
LOG_FILE = Path("./log/crawl_pages.log")

OUTPUT_DIR = ("./crawl_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CONFIG_DIR = Path("config")

def configure_logger() -> logging.Logger:
    # 進捗を画面とログファイルの両方へ出すロガーを初期化する。
    logger = logging.getLogger("crawl_pages")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


class CrawlPageLinks:
    def __init__(
        self,
        start_url: str,
        delay_seconds: float = 1,
        allowed_domains: list[str] | None = None,
        max_pages: int = 1000,
    ) -> None:
        self.start_url = start_url
        self.delay_seconds = delay_seconds
        self.max_pages = max_pages
        self.allowed_domains = {
            self._normalize_allowed_domain(domain)
            for domain in (allowed_domains or [])
            if self._normalize_allowed_domain(domain)
        }
        self.disallowed_domain_counts: Counter[str] = Counter()
        self.excluded_error_urls: list[str] = []
        self.logger = configure_logger()
        self.interactive_selector = ",".join(
            [
                "button",
                "[role='button']",
                "[aria-controls]",
                "[aria-expanded]",
                "[tabindex='0']",
            ]
        )

    # 実ブラウザに近い設定でコンテキストを作り、単純な bot 判定で弾かれにくくする。
    async def _new_browser_context(self, browser):
        return await browser.new_context(
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

    # 設定ファイルの許可ドメインは、URL形式や末尾スラッシュ付きでも比較できる形へ正規化する。
    def _normalize_allowed_domain(self, value: str) -> str:
        candidate = (value or "").strip().lower()
        if not candidate:
            return ""

        parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
        domain = (parsed.netloc or parsed.path).strip().strip("/")
        return domain.removeprefix("www.")

    # 読み込み直後のDOM変化を待って、クリックや抽出を安定させる。
    async def _wait_for_page_stable(self, page) -> None:
        await asyncio.sleep(self.delay_seconds)
        try:
            await page.wait_for_load_state("networkidle", timeout=5_000)
        except Exception:
            pass

    # 許可ドメインの一覧に含まれるURLだけを残す。
    def _is_allowed_link(self, url: str) -> bool:
        if not self.allowed_domains:
            return True

        domain = get_domain(url).removeprefix("www.")
        return domain in self.allowed_domains

    def _record_disallowed_link(self, url: str) -> None:
        domain = get_domain(url).removeprefix("www.")
        self.disallowed_domain_counts[domain or "<unknown>"] += 1

    # 現在の画面に見えているアンカーリンクを収集する。
    async def _collect_anchor_links(self, page) -> list[str]:
        hrefs = []
        anchors = await page.query_selector_all("a[href]")
        for anchor in anchors:
            href = await anchor.get_attribute("href")
            if href:
                hrefs.append(href)

        links = []
        seen = set()
        for href in hrefs:
            absolute_url = urljoin(page.url or self.start_url, href)
            normalized_url = normalize_url(absolute_url)
            if not self._is_allowed_link(normalized_url):
                self._record_disallowed_link(normalized_url)
                self.logger.info("skip disallowed link: %s", normalized_url)
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            links.append(normalized_url)

        self.logger.info("collected %s anchor links from %s", len(links), page.url)
        return links

    # ボタン押下で動的に表示・遷移した先から追加リンクを収集する。
    async def _collect_links_from_interactions(self, page, source_url: str) -> list[str]:
        discovered_links: list[str] = []
        seen = set()

        candidates = page.locator(self.interactive_selector)
        candidate_count = await candidates.count()
        self.logger.info("found %s interactive candidates on %s", candidate_count, source_url)

        for index in range(candidate_count):
            try:
                await page.goto(source_url, wait_until="load")
                await self._wait_for_page_stable(page)
            except Exception as exc:
                self.logger.warning("failed to reload interaction source: %s | %s", source_url, exc)
                break

            button = page.locator(self.interactive_selector).nth(index)
            try:
                if not await button.is_visible() or not await button.is_enabled():
                    self.logger.info("skip inactive candidate: index=%s", index)
                    continue
            except Exception:
                self.logger.info("skip unreadable candidate: index=%s", index)
                continue

            before_url = page.url
            try:
                button_text = ((await button.text_content()) or "").strip()
                self.logger.info("click candidate: index=%s text=%s", index, button_text or "<empty>")
                await button.scroll_into_view_if_needed(timeout=3_000)
                await button.click(timeout=5_000)
                await self._wait_for_page_stable(page)
            except Exception as exc:
                self.logger.warning("click failed: index=%s error=%s", index, exc)
                continue

            if page.url != before_url:
                normalized_url = normalize_url(page.url)
                if not self._is_allowed_link(normalized_url):
                    self._record_disallowed_link(normalized_url)
                    self.logger.info("skip disallowed navigated link: %s", normalized_url)
                elif normalized_url not in seen:
                    seen.add(normalized_url)
                    discovered_links.append(normalized_url)
                    self.logger.info("discovered navigated link: %s", normalized_url)

            for link in await self._collect_anchor_links(page):
                if link in seen:
                    continue
                seen.add(link)
                discovered_links.append(link)

        self.logger.info("collected %s interactive links", len(discovered_links))
        return discovered_links

    # 開始ページからリンク一覧を集めて正規化しながら重複を除く。
    async def _collect_links(self, page) -> list[str]:
        start_url = normalize_url(self.start_url)
        discovered_links: list[str] = []
        discovered_set: set[str] = set()
        visited_pages: set[str] = set()
        queued_pages: set[str] = {start_url}
        pages_to_visit = deque([start_url])

        self.logger.info("start collecting links from %s", start_url)

        while pages_to_visit and len(visited_pages) < self.max_pages:
            current_url = pages_to_visit.popleft()
            queued_pages.discard(current_url)
            if current_url in visited_pages:
                continue

            visited_pages.add(current_url)
            if current_url not in discovered_set:
                discovered_set.add(current_url)
                discovered_links.append(current_url)

            try:
                await page.goto(current_url, wait_until="load")
                await self._wait_for_page_stable(page)
            except Exception as exc:
                self.logger.warning("page visit failed: %s | %s", current_url, exc)
                continue

            page_links = await self._collect_anchor_links(page)
            page_links.extend(await self._collect_links_from_interactions(page, current_url))

            for normalized_url in page_links:
                if normalized_url not in discovered_set:
                    discovered_set.add(normalized_url)
                    discovered_links.append(normalized_url)
                if normalized_url in visited_pages or normalized_url in queued_pages:
                    continue
                queued_pages.add(normalized_url)
                pages_to_visit.append(normalized_url)

            self.logger.info(
                "crawl progress: visited=%s queued=%s discovered=%s current=%s",
                len(visited_pages),
                len(pages_to_visit),
                len(discovered_links),
                current_url,
            )

        if pages_to_visit:
            self.logger.warning(
                "stopped before queue was exhausted: visited=%s discovered=%s remaining=%s max_pages=%s",
                len(visited_pages),
                len(discovered_links),
                len(pages_to_visit),
                self.max_pages,
            )

        self.logger.info("link collection complete: %s total links", len(discovered_links))
        return discovered_links

    # 開始ページからURL一覧だけを取得する。
    async def fetch_links(self) -> list[str]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await self._new_browser_context(browser)
            page = await context.new_page()

            try:
                return await self._collect_links(page)
            finally:
                await page.close()
                await context.close()
                await browser.close()

    # 指定したURLを開いてページタイトルを取得する。
    async def _fetch_page_title(self, browser, url: str) -> str:
        context = await self._new_browser_context(browser)
        page = await context.new_page()
        try:
            self.logger.info("fetch title: %s", url)
            await page.goto(url, wait_until="load", timeout=60_000)
            await self._wait_for_page_stable(page)
            title = await page.title()
            title = title.strip()
            if title:
                self.logger.info("title fetched: %s | %s", url, title)
                return title

            fallback_title = await page.locator("h1").first.text_content()
            fallback_title = (fallback_title or "").strip()
            self.logger.info("fallback title fetched: %s | %s", url, fallback_title)
            return fallback_title
        except Exception as exc:
            self.logger.warning("title fetch failed: %s | %s", url, exc)
            return f"ERROR: {exc}"
        finally:
            await page.close()
            await context.close()

    def _is_valid_title(self, title: str) -> bool:
        normalized_title = title.strip()
        return bool(normalized_title) and not normalized_title.startswith("ERROR:")

    # URLとタイトルをドメイン別の辞書にまとめて返す。
    async def fetch_links_by_domain(self) -> tuple[dict[str, list[dict[str, str]]], int]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await self._new_browser_context(browser)
            listing_page = await context.new_page()

            try:
                links = await self._collect_links(listing_page)
                self.logger.info("start fetching titles for %s links", len(links))
                link_items: list[dict[str, str]] = []
                for url in links:
                    title = await self._fetch_page_title(browser, url)
                    if not self._is_valid_title(title):
                        self.excluded_error_urls.append(url)
                        self.logger.info("skip errored url: %s | %s", url, title)
                        continue
                    link_items.append({"url": url, "title": title})

                result = group_links_by_domain(link_items)
                total_count = count_links(result)
                self.logger.info("grouped into %s domains, %s links total", len(result), total_count)
                return result, total_count
            finally:
                await listing_page.close()
                await context.close()
                await browser.close()


def load_config(config_file: Path) -> dict:
    with config_file.open("r", encoding="utf-8") as config_input:
        config = json.load(config_input)

    start_url = config.get("start_page_url")
    allowed_domains = config.get("allowed_domains")
    max_pages = config.get("max_pages", 1000)
    output_file = config.get("output_file")

    if not isinstance(start_url, str) or not start_url.strip():
        raise ValueError("start_page_url must be a non-empty string.")
    if not isinstance(allowed_domains, list) or not all(isinstance(domain, str) and domain.strip() for domain in allowed_domains):
        raise ValueError("allowed_domains must be a non-empty list of strings.")
    if not isinstance(max_pages, int) or max_pages <= 0:
        raise ValueError("max_pages must be a positive integer.")
    if not isinstance(output_file, str) or not output_file.strip():
        raise ValueError("output_file must be a non-empty string.")

    normalized_allowed_domains = []
    for domain in allowed_domains:
        candidate = domain.strip().lower()
        parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
        normalized_domain = (parsed.netloc or parsed.path).strip().strip("/").removeprefix("www.")
        if normalized_domain:
            normalized_allowed_domains.append(normalized_domain)

    return {
        "start_url": start_url,
        "allowed_domains": normalized_allowed_domains,
        "max_pages": max_pages,
        "output_file": Path(output_file),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect links and titles with Playwright.")
    parser.add_argument(
        "--config",
        type=Path,
        default="crawl.jsonl",
        help="Crawler config JSON file path.",
    )
    return parser.parse_args()


async def get_links(start_url: str, allowed_domains: list[str], output_file: Path, max_pages: int):
    logger = configure_logger()
    crawler = CrawlPageLinks(
        start_url,
        allowed_domains=allowed_domains,
        max_pages=max_pages,
    )
    logger.info(
        "crawler started: start_url=%s allowed_domains=%s max_pages=%s",
        crawler.start_url,
        sorted(crawler.allowed_domains),
        crawler.max_pages,
    )
    links_by_domain, total_count = await crawler.fetch_links_by_domain()
    filepath = OUTPUT_DIR / output_file
    write_links_to_jsonl(links_by_domain, filepath)
    logger.info("saved jsonl: %s", filepath)
    print(f"count={total_count}")
    print(f"saved={output_file}")
    print(f"log={LOG_FILE}")
    for domain, items in links_by_domain.items():
        print(f"[{domain}] count={len(items)}")
        for item in items:
            print(f"{item['url']} | {item['title']}")
    if crawler.excluded_error_urls:
        print(f"[excluded error urls] count={len(crawler.excluded_error_urls)}")
        for url in crawler.excluded_error_urls:
            print(url)
    else:
        print("[excluded error urls] none")
    if crawler.disallowed_domain_counts:
        print("[excluded domains]")
        for domain, count in crawler.disallowed_domain_counts.most_common():
            print(f"{domain} | skipped={count}")
    else:
        print("[excluded domains] none")



if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.config)
    asyncio.run(
        get_links(
            start_url=config["start_url"],
            allowed_domains=config["allowed_domains"],
            output_file=config["output_file"],
            max_pages=config["max_pages"],
        )
    )
