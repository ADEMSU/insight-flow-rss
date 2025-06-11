from __future__ import annotations

"""A single, consistently‑typed representation of any article or message that
flows through InsightFlow.  This module **must stay absolutely independent** of
external services (RSS, Cubus, Telegram &c.) so that every other component can
rely on a clean, stable interface.

Key design goals
================
* **Minimal required fields** – the object is valid as long as it carries an
  *id* and at least *some* textual payload.
* **Strict types** – everything is validated on construction with *Pydantic* so
  we never pass half‑broken data further down the pipeline.
* **Portable JSON shape** – :pymeth:`Post.to_record` returns exactly the subset
  used by the DB layer and by inter‑service communication.
* **Helper constructors** – small factory methods turn raw RSS / Cubus / custom
  dictionaries into proper, validated :class:`Post` instances.

If you need to extend the schema, first check that the new field is indeed
shared by *every* data source; otherwise keep it in :pyattr:`Post.raw`.
"""

from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, List, Optional

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
#  Constants / configuration -------------------------------------------------
# ---------------------------------------------------------------------------

_MIN_TITLE_WORDS = 3  # how many words make a meaningful title
_MAX_TITLE_LEN = 120  # soft cap to keep titles reasonably short


# ---------------------------------------------------------------------------
#  Enumerations --------------------------------------------------------------
# ---------------------------------------------------------------------------

class BlogHostType(IntEnum):
    """Very rough classification of a source host."""

    OTHER = 0
    BLOG = 1
    MICROBLOG = 2
    SOCIAL = 3
    FORUM = 4
    MEDIA = 5
    REVIEW = 6
    MESSENGER = 7


# ---------------------------------------------------------------------------
#  The *Post* model ----------------------------------------------------------
# ---------------------------------------------------------------------------

class Post(BaseModel):
    """Single post/article/item flowing through the pipeline."""

    # ----- primary fields (persisted) --------------------------------------
    post_id: str = Field(..., description="Unique, stable identifier")
    content: Optional[str] = Field(None, description="Cleaned plain text body")
    html_content: Optional[str] = Field(
        None, description="Original rich body – **not** used for ML/DB"
    )
    title: str = Field("", description="Best effort title")

    blog_host: Optional[str] = Field(None, description="example.com / rss.nyt.com …")
    blog_host_type: BlogHostType = Field(BlogHostType.OTHER)

    url: str = Field("", description="Canonical permalink if any")
    published_on: Optional[datetime] = Field(None, description="tz‑aware timestamp")

    simhash: Optional[str] = Field(None, description="Computed simhash for dedup")
    object_ids: List[str] = Field(default_factory=list, description="Cubus object ids")

    # ----- everything else stays here --------------------------------------
    raw: Optional[Any] = Field(None, exclude=True, description="Untouched original")

    # ------------------------------------------------------------------
    #  Validators / normalisers ----------------------------------------
    # ------------------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def _ensure_post_id(cls, values: dict[str, Any]):
        """Guarantee *post_id* – fallback to hash of url/title if needed."""
        if not values.get("post_id"):
            fallback = values.get("url") or values.get("title")
            if fallback:
                import hashlib

                values["post_id"] = hashlib.md5(fallback.encode()).hexdigest()
                logger.debug("post_id generated from fallback value")
            else:
                raise ValueError("post_id, url or title must be provided")
        return values

    @field_validator("title", mode="after")
    def _tidy_title(cls, v: str):  # noqa: D401
        """Trim and truncate extremely long titles."""
        v = v.strip()
        return (v[: _MAX_TITLE_LEN] + "…") if len(v) > _MAX_TITLE_LEN else v

    # ------------------------------------------------------------------
    #  Public helpers ---------------------------------------------------
    # ------------------------------------------------------------------

    def to_record(self) -> dict[str, Any]:
        """Serialise **only** fields required by the DB layer / JSON API."""
        return {
            "post_id": self.post_id,
            "object_ids": self.object_ids,
            "content": self.content,
            "html_content": self.html_content,
            "blog_host": self.blog_host,
            "blog_host_type": self.blog_host_type.value,
            "published_on": self.published_on,
            "simhash": self.simhash,
            "url": self.url,
            "title": self.title,
        }

    # ------------------------------------------------------------------
    #  Factory helpers --------------------------------------------------
    # ------------------------------------------------------------------

    @classmethod
    def from_rss(cls, entry: dict[str, Any]) -> "Post":
        """Create a :class:`Post` from a *feedparser* entry dict."""
        published = None
        if "published_parsed" in entry and entry.published_parsed:
            import time
            from zoneinfo import ZoneInfo

            published = datetime.fromtimestamp(
                time.mktime(entry.published_parsed), tz=ZoneInfo("UTC")
            )
        elif entry.get("published"):
            try:
                published = datetime.fromisoformat(entry["published"])
            except Exception:  # noqa: BLE001
                pass

        return cls(
            post_id=entry.get("id") or entry.get("guid") or entry.get("link", ""),
            title=entry.get("title", ""),
            url=entry.get("link", ""),
            content=entry.get("summary", ""),
            html_content=entry.get("summary_detail", {}).get("value"),
            blog_host=entry.get("source_name"),
            blog_host_type=BlogHostType.MEDIA,
            published_on=published,
            raw=entry,
        )

    # ------------------------------------------------------------------
    #  Niceties ---------------------------------------------------------
    # ------------------------------------------------------------------

    def __str__(self) -> str:  # pragma: no cover – debug aid
        return f"<{self.post_id} | {self.title[:50]!r}>"

    def __hash__(self) -> int:  # ensure the model is hashable
        return hash(self.post_id)

    def pretty(self) -> str:  # pragma: no cover – debug aid
        """Return a formatted representation for log/output usage."""
        return pformat(self.to_record(), compact=True, width=120)


# ---------------------------------------------------------------------------
#  Convenience plural helpers ------------------------------------------------
# ---------------------------------------------------------------------------


def parse_rss_entries(entries: list[dict[str, Any]]) -> list[Post]:
    """Convert a raw *feedparser* entries list into validated :class:`Post`s."""
    posts: list[Post] = []
    for entry in entries:
        try:
            posts.append(Post.from_rss(entry))
        except Exception as exc:  # pragma: no cover
            logger.error("Cannot parse RSS entry: {}", exc, exc_info=True)
    logger.info("Converted {} RSS entries to Post objects", len(posts))
    return posts
