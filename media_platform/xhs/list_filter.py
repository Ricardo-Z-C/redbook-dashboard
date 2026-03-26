# -*- coding: utf-8 -*-
import json
import os
from typing import Dict, Tuple

import httpx

import config
from tools import utils


def _build_candidate_text(post_item: Dict) -> str:
    title = str(post_item.get("title", "") or post_item.get("display_title", "") or "").strip()
    desc = str(post_item.get("desc", "") or post_item.get("content", "") or "").strip()
    model_type = str(post_item.get("model_type", "") or "").strip()
    note_id = str(post_item.get("id", "") or "").strip()
    return (
        f"note_id: {note_id}\n"
        f"model_type: {model_type}\n"
        f"title: {title}\n"
        f"desc: {desc}\n"
    )


async def ai_allow_search_item(keyword: str, condition: str, post_item: Dict) -> Tuple[bool, str]:
    """
    Return (allow, reason). allow=True means keep this list item for detail fetch.
    """
    api_key = str(config.LIST_FILTER_API_KEY or os.getenv("XHS_LIST_FILTER_API_KEY", "")).strip()
    base_url = str(
        os.getenv("XHS_LIST_FILTER_API_BASE", "")
        or config.LIST_FILTER_API_BASE
        or "https://api.openai.com/v1"
    ).rstrip("/")
    model = str(
        os.getenv("XHS_LIST_FILTER_MODEL", "")
        or config.LIST_FILTER_MODEL
        or "gpt-4o-mini"
    ).strip()
    timeout_sec = int(config.LIST_FILTER_TIMEOUT_SEC or 20)

    if not condition.strip():
        return True, "empty_condition"
    if not api_key:
        utils.logger.warning(
            "[xhs.list_filter] LIST_FILTER_CONDITION is set but API key is empty, skip AI filtering."
        )
        return True, "missing_api_key"

    candidate_text = _build_candidate_text(post_item)
    prompt = (
        "你是一个严格的内容筛选器。请根据用户要求，判断候选帖子是否应该继续爬取详情。\n"
        "只输出一个 JSON 对象，不要输出额外文字。\n"
        '格式：{"keep": true/false, "reason": "简短原因"}\n\n'
        f"搜索关键词：{keyword}\n"
        f"用户过滤条件：{condition}\n\n"
        f"候选帖子信息：\n{candidate_text}\n"
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你只输出 JSON，不要输出其他内容。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post(f"{base_url}/chat/completions", json=payload, headers=headers)
            resp.raise_for_status()
            body = resp.json()
        content = (
            body.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
    except Exception as e:
        utils.logger.warning(f"[xhs.list_filter] AI list filter request failed, fallback keep=True: {e}")
        return True, "request_failed"

    keep = True
    reason = "model_default_keep"
    try:
        if content.startswith("```"):
            content = content.strip("`")
            if content.startswith("json"):
                content = content[4:].strip()
        parsed = json.loads(content)
        keep = bool(parsed.get("keep", True))
        reason = str(parsed.get("reason", ""))
    except Exception:
        lowered = content.lower()
        if '"keep": false' in lowered or lowered in {"false", "no", "reject"}:
            keep = False
            reason = "plain_text_false"
        else:
            keep = True
            reason = "plain_text_true_or_unknown"

    return keep, reason
