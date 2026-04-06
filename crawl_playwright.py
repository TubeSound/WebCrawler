import asyncio
import argparse
import logging
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import async_playwright
from libs_crawl import count_links, get_domain, group_links_by_domain, normalize_url, write_links_to_jsonl


LOG_FILE = Path("crawl_playwright.log")
DEFAULT_START_URL = ""
DEFAULT_ALLOWED_DOMAINS = [""]
DEFAULT_OUTPUT_FILE = Path("")


def configure_logger() -> logging.Logger:
    # 進捗を画面とログファイルの両方へ出すロガーを初期化する。
    logger = logging.getLogger("crawl_playwright")
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


class PlaywrightLinkCrawler:
    def __init__(
        self,
        start_url: str,
        delay_seconds: float = 1,
        allowed_domains: list[str] | None = None,
    ) -> None:
        self.start_url = start_url
        self.delay_seconds = delay_seconds
        self.allowed_domains = {
            domain.lower().removeprefix("www.")
            for domain in (allowed_domains or [])
        }
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
                self.logger.info("skip disallowed link: %s", normalized_url)
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            links.append(normalized_url)

        self.logger.info("collected %s anchor links from %s", len(links), page.url)
        return links

    # ボタン押下で動的に表示・遷移した先から追加リンクを収集する。
    async def _collect_links_from_interactions(self, page) -> list[str]:
        discovered_links: list[str] = []
        seen = set()

        candidates = page.locator(self.interactive_selector)
        candidate_count = await candidates.count()
        self.logger.info("found %s interactive candidates on %s", candidate_count, self.start_url)

        for index in range(candidate_count):
            await page.goto(self.start_url, wait_until="load")
            await self._wait_for_page_stable(page)

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
                if self._is_allowed_link(normalized_url) and normalized_url not in seen:
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
        self.logger.info("start collecting links from %s", self.start_url)
        await page.goto(self.start_url, wait_until="load")
        await self._wait_for_page_stable(page)

        links = []
        seen = set()
        for normalized_url in await self._collect_anchor_links(page):
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            links.append(normalized_url)

        self.logger.info("anchor collection complete: %s links", len(links))
        for normalized_url in await self._collect_links_from_interactions(page):
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            links.append(normalized_url)

        self.logger.info("link collection complete: %s total links", len(links))
        return links

    # 開始ページからURL一覧だけを取得する。
    async def fetch_links(self) -> list[str]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                return await self._collect_links(page)
            finally:
                await page.close()
                await browser.close()

    # 指定したURLを開いてページタイトルを取得する。
    async def _fetch_page_title(self, browser, url: str) -> str:
        page = await browser.new_page()
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

    # URLとタイトルをドメイン別の辞書にまとめて返す。
    async def fetch_links_by_domain(self) -> tuple[dict[str, list[dict[str, str]]], int]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            listing_page = await browser.new_page()

            try:
                links = await self._collect_links(listing_page)
                self.logger.info("start fetching titles for %s links", len(links))
                link_items: list[dict[str, str]] = []
                for url in links:
                    title = await self._fetch_page_title(browser, url)
                    link_items.append({"url": url, "title": title})

                result = group_links_by_domain(link_items)
                total_count = count_links(result)
                self.logger.info("grouped into %s domains, %s links total", len(result), total_count)
                return result, total_count
            finally:
                await listing_page.close()
                await browser.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect links and titles with Playwright.")
    parser.add_argument(
        "--start-url",
        default=DEFAULT_START_URL,
        help="Page URL to start crawling from.",
    )
    parser.add_argument(
        "--allowed-domain",
        action="append",
        dest="allowed_domains",
        help="Allowed domain. Repeat this option to allow multiple domains.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="Output JSONL file path.",
    )
    return parser.parse_args()


async def get_links(start_url: str, allowed_domains: list[str], output_file: Path):
    logger = configure_logger()
    crawler = PlaywrightLinkCrawler(
        start_url,
        allowed_domains=allowed_domains,
    )
    logger.info("crawler started: start_url=%s allowed_domains=%s", crawler.start_url, sorted(crawler.allowed_domains))
    links_by_domain, total_count = await crawler.fetch_links_by_domain()
    write_links_to_jsonl(links_by_domain, output_file)
    logger.info("saved jsonl: %s", output_file)
    print(f"count={total_count}")
    print(f"saved={output_file}")
    print(f"log={LOG_FILE}")
    for domain, items in links_by_domain.items():
        print(f"[{domain}] count={len(items)}")
        for item in items:
            print(f"{item['url']} | {item['title']}")



if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        get_links(
            start_url=args.start_url,
            allowed_domains=args.allowed_domains or DEFAULT_ALLOWED_DOMAINS,
            output_file=args.output,
        )
    )
