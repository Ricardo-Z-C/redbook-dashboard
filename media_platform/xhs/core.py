# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/media_platform/xhs/core.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional

from playwright.async_api import (
    BrowserContext,
    BrowserType,
    Page,
    Playwright,
    async_playwright,
)
from tenacity import RetryError

import config
from base.base_crawler import AbstractCrawler
from model.m_xiaohongshu import NoteUrlInfo, CreatorUrlInfo
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import xhs as xhs_store
from tools import utils
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import XiaoHongShuClient
from .exception import DataFetchError, NoteNotFoundError
from .field import SearchSortType
from .help import parse_note_info_from_note_url, parse_creator_info_from_url, get_search_id
from .list_filter import ai_allow_search_item
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    context_page: Page
    xhs_client: XiaoHongShuClient
    browser_context: BrowserContext
    cdp_manager: Optional[CDPBrowserManager]

    def __init__(self) -> None:
        self.index_url = "https://www.xiaohongshu.com"
        # self.user_agent = utils.get_user_agent()
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        self.cdp_manager = None
        self.ip_proxy_pool = None  # Proxy IP pool for automatic proxy refresh

    async def start(self) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            self.ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await self.ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = utils.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            # Choose launch mode based on configuration
            if config.ENABLE_CDP_MODE:
                utils.logger.info("[XiaoHongShuCrawler] Launching browser using CDP mode")
                self.browser_context = await self.launch_browser_with_cdp(
                    playwright,
                    playwright_proxy_format,
                    self.user_agent,
                    headless=config.CDP_HEADLESS,
                )
            else:
                utils.logger.info("[XiaoHongShuCrawler] Launching browser using standard mode")
                # Launch a browser context.
                chromium = playwright.chromium
                self.browser_context = await self.launch_browser(
                    chromium,
                    playwright_proxy_format,
                    self.user_agent,
                    headless=config.HEADLESS,
                )
                # stealth.min.js is a js script to prevent the website from detecting the crawler.
                await self.browser_context.add_init_script(path="libs/stealth.min.js")

            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            # Create a client to interact with the Xiaohongshu website.
            self.xhs_client = await self.create_xhs_client(httpx_proxy_format)
            if not await self.xhs_client.pong():
                login_obj = XiaoHongShuLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # input your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES,
                )
                await login_obj.begin()
                await self.xhs_client.update_cookies(browser_context=self.browser_context)

            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for notes and retrieve their comment information.
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                await self.get_specified_notes()
            elif config.CRAWLER_TYPE == "creator":
                # Get creator's information and their notes and comments
                await self.get_creators_and_notes()
            else:
                pass

            utils.logger.info("[XiaoHongShuCrawler.start] Xhs Crawler finished ...")

    async def search(self) -> None:
        """Search for notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.search] Begin search Xiaohongshu keywords")
        xhs_limit_count = 20  # Xiaohongshu limit page fixed value
        # if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
        #     config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}")
            page = 1
            collected_note_count = 0
            search_id = get_search_id()
            # Ensure at least one page is crawled when max count is below platform page size (20).
            while (page - start_page) * xhs_limit_count < config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue
                if collected_note_count >= config.CRAWLER_MAX_NOTES_COUNT:
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search] Reached target count={config.CRAWLER_MAX_NOTES_COUNT}, stop crawling keyword={keyword}"
                    )
                    break

                try:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] search Xiaohongshu keyword: {keyword}, page: {page}")
                    note_ids: List[str] = []
                    xsec_tokens: List[str] = []
                    notes_res = await self.xhs_client.get_note_by_keyword(
                        keyword=keyword,
                        search_id=search_id,
                        page=page,
                        sort=(SearchSortType(config.SORT_TYPE) if config.SORT_TYPE != "" else SearchSortType.GENERAL),
                    )
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Search notes response: {notes_res}")
                    if not notes_res or not notes_res.get("has_more", False):
                        utils.logger.info("[XiaoHongShuCrawler.search] No more content!")
                        break

                    filtered_items = [
                        post_item
                        for post_item in notes_res.get("items", {})
                        if post_item.get("model_type") not in ("rec_query", "hot_query")
                    ]
                    if config.LIST_FILTER_CONDITION:
                        keep_items: List[Dict] = []
                        skipped_count = 0
                        for post_item in filtered_items:
                            keep, reason = await ai_allow_search_item(
                                keyword=keyword,
                                condition=config.LIST_FILTER_CONDITION,
                                post_item=post_item,
                            )
                            if keep:
                                keep_items.append(post_item)
                            else:
                                skipped_count += 1
                                utils.logger.info(
                                    f"[XiaoHongShuCrawler.search] list item skipped by AI filter, note_id={post_item.get('id')}, reason={reason}"
                                )
                        filtered_items = keep_items
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.search] AI list filter enabled, kept={len(filtered_items)}, skipped={skipped_count}, page={page}"
                        )
                    remaining_quota = max(config.CRAWLER_MAX_NOTES_COUNT - collected_note_count, 0)
                    if remaining_quota <= 0:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.search] No remaining quota, stop crawling keyword={keyword}"
                        )
                        break
                    if len(filtered_items) > remaining_quota:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.search] Truncate page items from {len(filtered_items)} to remaining quota {remaining_quota}"
                        )
                        filtered_items = filtered_items[:remaining_quota]

                    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
                    task_list = [
                        self.get_note_detail_async_task(
                            note_id=post_item.get("id"),
                            xsec_source=post_item.get("xsec_source"),
                            xsec_token=post_item.get("xsec_token"),
                            semaphore=semaphore,
                        )
                        for post_item in filtered_items
                    ]
                    note_details = await asyncio.gather(*task_list)
                    for note_detail in note_details:
                        if note_detail:
                            await xhs_store.update_xhs_note(note_detail)
                            await self.get_notice_media(note_detail)
                            note_ids.append(note_detail.get("note_id"))
                            xsec_tokens.append(note_detail.get("xsec_token"))
                    collected_note_count += len(note_ids)
                    page += 1
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Note details: {note_details}")
                    await self.batch_get_note_comments(note_ids, xsec_tokens)
                    if collected_note_count >= config.CRAWLER_MAX_NOTES_COUNT:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.search] Reached target count={collected_note_count}, stop crawling keyword={keyword}"
                        )
                        break

                    # Sleep after each page navigation
                    await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}")
                except DataFetchError:
                    utils.logger.error("[XiaoHongShuCrawler.search] Get note detail error")
                    break

    async def get_creators_and_notes(self) -> None:
        """Get creator's notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.get_creators_and_notes] Begin get Xiaohongshu creators")
        for creator_url in config.XHS_CREATOR_ID_LIST:
            try:
                # Parse creator URL to get user_id and security tokens
                creator_info: CreatorUrlInfo = parse_creator_info_from_url(creator_url)
                utils.logger.info(f"[XiaoHongShuCrawler.get_creators_and_notes] Parse creator URL info: {creator_info}")
                user_id = creator_info.user_id

                # get creator detail info from web html content
                createor_info: Dict = await self.xhs_client.get_creator_info(
                    user_id=user_id,
                    xsec_token=creator_info.xsec_token,
                    xsec_source=creator_info.xsec_source
                )
                if createor_info:
                    await xhs_store.save_creator(user_id, creator=createor_info)
            except ValueError as e:
                utils.logger.error(f"[XiaoHongShuCrawler.get_creators_and_notes] Failed to parse creator URL: {e}")
                continue

            # Use fixed crawling interval
            crawl_interval = config.CRAWLER_MAX_SLEEP_SEC
            # Get all note information of the creator
            all_notes_list = await self.xhs_client.get_all_notes_by_creator(
                user_id=user_id,
                crawl_interval=crawl_interval,
                callback=self.fetch_creator_notes_detail,
                xsec_token=creator_info.xsec_token,
                xsec_source=creator_info.xsec_source,
            )

            note_ids = []
            xsec_tokens = []
            for note_item in all_notes_list:
                note_ids.append(note_item.get("note_id"))
                xsec_tokens.append(note_item.get("xsec_token"))
            await self.batch_get_note_comments(note_ids, xsec_tokens)

    async def fetch_creator_notes_detail(self, note_list: List[Dict]):
        """Concurrently obtain the specified post list and save the data"""
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source=post_item.get("xsec_source"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            ) for post_item in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail:
                await xhs_store.update_xhs_note(note_detail)
                await self.get_notice_media(note_detail)

    async def get_specified_notes(self):
        """Get the information and comments of the specified post

        Note: Must specify note_id, xsec_source, xsec_token
        """
        get_note_detail_task_list = []
        for full_note_url in config.XHS_SPECIFIED_NOTE_URL_LIST:
            note_url_info: NoteUrlInfo = parse_note_info_from_note_url(full_note_url)
            utils.logger.info(f"[XiaoHongShuCrawler.get_specified_notes] Parse note url info: {note_url_info}")
            crawler_task = self.get_note_detail_async_task(
                note_id=note_url_info.note_id,
                xsec_source=note_url_info.xsec_source,
                xsec_token=note_url_info.xsec_token,
                semaphore=asyncio.Semaphore(config.MAX_CONCURRENCY_NUM),
            )
            get_note_detail_task_list.append(crawler_task)

        need_get_comment_note_ids = []
        xsec_tokens = []
        note_details = await asyncio.gather(*get_note_detail_task_list)
        for note_detail in note_details:
            if note_detail:
                need_get_comment_note_ids.append(note_detail.get("note_id", ""))
                xsec_tokens.append(note_detail.get("xsec_token", ""))
                await xhs_store.update_xhs_note(note_detail)
                await self.get_notice_media(note_detail)
        await self.batch_get_note_comments(need_get_comment_note_ids, xsec_tokens)

    async def get_note_detail_async_task(
        self,
        note_id: str,
        xsec_source: str,
        xsec_token: str,
        semaphore: asyncio.Semaphore,
    ) -> Optional[Dict]:
        """Get note detail

        Args:
            note_id:
            xsec_source:
            xsec_token:
            semaphore:

        Returns:
            Dict: note detail
        """
        note_detail = None
        utils.logger.info(f"[get_note_detail_async_task] Begin get note detail, note_id: {note_id}")
        async with semaphore:
            try:
                try:
                    note_detail = await self.xhs_client.get_note_by_id(note_id, xsec_source, xsec_token)
                except RetryError:
                    pass

                if not note_detail:
                    note_detail = await self.xhs_client.get_note_by_id_from_html(note_id, xsec_source, xsec_token,
                                                                                 enable_cookie=True)
                    if not note_detail:
                        raise Exception(f"[get_note_detail_async_task] Failed to get note detail, Id: {note_id}")

                note_detail.update({"xsec_token": xsec_token, "xsec_source": xsec_source})

                # Sleep after fetching note detail
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[get_note_detail_async_task] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching note {note_id}")

                return note_detail

            except NoteNotFoundError as ex:
                utils.logger.warning(f"[XiaoHongShuCrawler.get_note_detail_async_task] Note not found: {note_id}, {ex}")
                return None
            except DataFetchError as ex:
                utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail_async_task] have not fund note detail note_id:{note_id}, err: {ex}")
                return None

    async def batch_get_note_comments(self, note_list: List[str], xsec_tokens: List[str]):
        """Batch get note comments"""
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for index, note_id in enumerate(note_list):
            task = asyncio.create_task(
                self.get_comments(note_id=note_id, xsec_token=xsec_tokens[index], semaphore=semaphore),
                name=note_id,
            )
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_comments(self, note_id: str, xsec_token: str, semaphore: asyncio.Semaphore):
        """Get note comments with keyword filtering and quantity limitation"""
        async with semaphore:
            utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}")
            # Use fixed crawling interval
            crawl_interval = config.CRAWLER_MAX_SLEEP_SEC
            await self.xhs_client.get_note_all_comments(
                note_id=note_id,
                xsec_token=xsec_token,
                crawl_interval=crawl_interval,
                callback=xhs_store.batch_update_xhs_note_comments,
                max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
            )

            # Sleep after fetching comments
            await asyncio.sleep(crawl_interval)
            utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Sleeping for {crawl_interval} seconds after fetching comments for note {note_id}")

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """Create Xiaohongshu client"""
        utils.logger.info("[XiaoHongShuCrawler.create_xhs_client] Begin create Xiaohongshu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        xhs_client_obj = XiaoHongShuClient(
            proxy=httpx_proxy,
            headers={
                "accept": "application/json, text/plain, */*",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/json;charset=UTF-8",
                "origin": "https://www.xiaohongshu.com",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "referer": "https://www.xiaohongshu.com/",
                "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
                "Cookie": cookie_str,
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
            proxy_ip_pool=self.ip_proxy_pool,  # Pass proxy pool for automatic refresh
        )
        return xhs_client_obj

    async def launch_browser(
        self,
        chromium: BrowserType,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        utils.logger.info("[XiaoHongShuCrawler.launch_browser] Begin create browser context ...")
        os.environ.setdefault("PLAYWRIGHT_DISABLE_CRASH_REPORTER", "1")
        # macOS environments may block Crashpad xattr writes in default locations.
        # Force crash dumps to a writable project-local path and disable crash reporter.
        crashpad_dir = os.path.join(os.getcwd(), ".runtime_home", "crashpad")
        os.makedirs(crashpad_dir, exist_ok=True)
        launch_args = [
            "--disable-crashpad-for-testing",
            "--disable-crash-reporter",
            f"--crash-dumps-dir={crashpad_dir}",
            "--disable-gpu",
        ]
        if config.SAVE_LOGIN_STATE:
            # feat issue #14
            # we will save login state to avoid login every time
            user_data_dir = os.path.join(os.getcwd(), "browser_data", config.USER_DATA_DIR % config.PLATFORM)  # type: ignore
            launch_options = {
                "user_data_dir": user_data_dir,
                "accept_downloads": True,
                "headless": headless,
                "proxy": playwright_proxy,  # type: ignore
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": user_agent,
                "args": launch_args,
            }
            try:
                return await self._await_with_browser_timeout(
                    chromium.launch_persistent_context(**launch_options),
                    "launch_persistent_context",
                )
            except Exception as default_error:
                if self._is_launch_timeout_error(default_error):
                    utils.logger.warning(
                        "[XiaoHongShuCrawler.launch_browser] Default persistent profile launch timed out, retrying with fallback profile"
                    )
                    fallback_options = dict(launch_options)
                    fallback_options["user_data_dir"] = self._build_fallback_user_data_dir("timeout_fallback")
                    try:
                        return await self._await_with_browser_timeout(
                            chromium.launch_persistent_context(**fallback_options),
                            "launch_persistent_context(fallback_profile)",
                        )
                    except Exception as fallback_error:
                        utils.logger.warning(
                            f"[XiaoHongShuCrawler.launch_browser] Fallback profile launch failed: {fallback_error}"
                        )
                        default_error = fallback_error
                if not self._is_crashpad_launch_error(default_error):
                    raise
                utils.logger.warning(f"[XiaoHongShuCrawler.launch_browser] Default Chromium launch failed: {default_error}")
                for channel in ("chrome", "msedge"):
                    try:
                        utils.logger.info(f"[XiaoHongShuCrawler.launch_browser] Retrying with channel={channel}")
                        channel_options = dict(launch_options)
                        channel_options["user_data_dir"] = self._build_fallback_user_data_dir(f"channel_{channel}")
                        return await self._await_with_browser_timeout(
                            chromium.launch_persistent_context(channel=channel, **channel_options),
                            f"launch_persistent_context(channel={channel})",
                        )
                    except Exception as channel_error:
                        utils.logger.warning(
                            f"[XiaoHongShuCrawler.launch_browser] channel={channel} launch failed: {channel_error}"
                        )
                for browser_path in self._fallback_browser_paths():
                    try:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.launch_browser] Retrying with executable_path={browser_path}"
                        )
                        path_options = dict(launch_options)
                        safe_name = os.path.basename(browser_path).replace(" ", "_")
                        path_options["user_data_dir"] = self._build_fallback_user_data_dir(f"path_{safe_name}")
                        return await self._await_with_browser_timeout(
                            chromium.launch_persistent_context(executable_path=browser_path, **path_options),
                            f"launch_persistent_context(executable_path={browser_path})",
                        )
                    except Exception as path_error:
                        utils.logger.warning(
                            f"[XiaoHongShuCrawler.launch_browser] executable_path={browser_path} launch failed: {path_error}"
                        )
                raise
        else:
            launch_options = {
                "headless": headless,
                "proxy": playwright_proxy,
                "args": launch_args,
            }
            try:
                browser = await self._await_with_browser_timeout(
                    chromium.launch(**launch_options),  # type: ignore
                    "launch",
                )
            except Exception as default_error:
                if not self._is_crashpad_launch_error(default_error):
                    raise
                utils.logger.warning(f"[XiaoHongShuCrawler.launch_browser] Default Chromium launch failed: {default_error}")
                browser = None
                for channel in ("chrome", "msedge"):
                    try:
                        utils.logger.info(f"[XiaoHongShuCrawler.launch_browser] Retrying with channel={channel}")
                        browser = await self._await_with_browser_timeout(
                            chromium.launch(channel=channel, **launch_options),  # type: ignore
                            f"launch(channel={channel})",
                        )
                        break
                    except Exception as channel_error:
                        utils.logger.warning(
                            f"[XiaoHongShuCrawler.launch_browser] channel={channel} launch failed: {channel_error}"
                        )
                if browser is None:
                    for browser_path in self._fallback_browser_paths():
                        try:
                            utils.logger.info(
                                f"[XiaoHongShuCrawler.launch_browser] Retrying with executable_path={browser_path}"
                            )
                            browser = await self._await_with_browser_timeout(
                                chromium.launch(executable_path=browser_path, **launch_options),  # type: ignore
                                f"launch(executable_path={browser_path})",
                            )
                            break
                        except Exception as path_error:
                            utils.logger.warning(
                                f"[XiaoHongShuCrawler.launch_browser] executable_path={browser_path} launch failed: {path_error}"
                            )
                if browser is None:
                    raise
            browser_context = await self._await_with_browser_timeout(
                browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=user_agent),
                "browser.new_context",
            )
            return browser_context

    async def _await_with_browser_timeout(self, awaitable, action_name: str):
        timeout_sec = self._resolve_browser_launch_timeout_sec()
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout_sec)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"[XiaoHongShuCrawler.launch_browser] {action_name} timeout after {timeout_sec}s"
            ) from exc

    @staticmethod
    def _resolve_browser_launch_timeout_sec() -> int:
        raw = str(os.getenv("BROWSER_LAUNCH_TIMEOUT_SEC", "90")).strip()
        try:
            value = int(raw)
        except ValueError:
            value = 90
        return max(20, min(value, 300))

    @staticmethod
    def _build_fallback_user_data_dir(tag: str = "fallback") -> str:
        base_dir = os.path.join(os.getcwd(), ".runtime_home", "xhs_user_data_fallback")
        safe_tag = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(tag or "fallback"))
        fallback_dir = f"{base_dir}_{safe_tag}_{os.getpid()}"
        os.makedirs(fallback_dir, exist_ok=True)
        return fallback_dir

    @staticmethod
    def _is_launch_timeout_error(error: Exception) -> bool:
        if isinstance(error, TimeoutError):
            return True
        details = str(error).lower()
        return "timeout after" in details or "timed out" in details

    @staticmethod
    def _is_crashpad_launch_error(error: Exception) -> bool:
        details = str(error)
        crashpad_signals = (
            "Operation not permitted",
            "Crashpad",
            "crashpad",
            "xattr.cc",
            "TargetClosedError",
        )
        return any(signal in details for signal in crashpad_signals)

    @staticmethod
    def _fallback_browser_paths() -> List[str]:
        candidates = (
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        )
        return [path for path in candidates if os.path.isfile(path) and os.access(path, os.X_OK)]

    async def launch_browser_with_cdp(
        self,
        playwright: Playwright,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """Launch browser using CDP mode"""
        try:
            self.cdp_manager = CDPBrowserManager()
            browser_context = await self.cdp_manager.launch_and_connect(
                playwright=playwright,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
                headless=headless,
            )

            # Display browser information
            browser_info = await self.cdp_manager.get_browser_info()
            utils.logger.info(f"[XiaoHongShuCrawler] CDP browser info: {browser_info}")

            return browser_context

        except Exception as e:
            utils.logger.error(f"[XiaoHongShuCrawler] CDP mode launch failed, falling back to standard mode: {e}")
            # Fall back to standard mode
            chromium = playwright.chromium
            return await self.launch_browser(chromium, playwright_proxy, user_agent, headless)

    async def close(self):
        """Close browser context"""
        # Special handling if using CDP mode
        if self.cdp_manager:
            await self.cdp_manager.cleanup()
            self.cdp_manager = None
        else:
            await self.browser_context.close()
        utils.logger.info("[XiaoHongShuCrawler.close] Browser context closed ...")

    async def get_notice_media(self, note_detail: Dict):
        if not config.ENABLE_GET_MEIDAS:
            utils.logger.info(f"[XiaoHongShuCrawler.get_notice_media] Crawling image mode is not enabled")
            return
        await self.get_note_images(note_detail)
        await self.get_notice_video(note_detail)

    async def get_note_images(self, note_item: Dict):
        """Get note images. Please use get_notice_media

        Args:
            note_item: Note item dictionary
        """
        if not config.ENABLE_GET_MEIDAS:
            return
        note_id = note_item.get("note_id")
        image_list: List[Dict] = note_item.get("image_list", [])

        for img in image_list:
            if img.get("url_default") != "":
                img.update({"url": img.get("url_default")})

        if not image_list:
            return
        picNum = 0
        for pic in image_list:
            url = pic.get("url")
            if not url:
                continue
            content = await self.xhs_client.get_note_media(url)
            await asyncio.sleep(random.random())
            if content is None:
                continue
            extension_file_name = f"{picNum}.jpg"
            picNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

    async def get_notice_video(self, note_item: Dict):
        """Get note videos. Please use get_notice_media

        Args:
            note_item: Note item dictionary
        """
        if not config.ENABLE_GET_MEIDAS:
            return
        note_id = note_item.get("note_id")

        videos = xhs_store.get_video_url_arr(note_item)

        if not videos:
            return
        videoNum = 0
        for url in videos:
            content = await self.xhs_client.get_note_media(url)
            await asyncio.sleep(random.random())
            if content is None:
                continue
            extension_file_name = f"{videoNum}.mp4"
            videoNum += 1
            await xhs_store.update_xhs_note_video(note_id, content, extension_file_name)
