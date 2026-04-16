"""
sync_knowledge_base.py

Syncs Notion workspace pages into Supabase vector tables for RAG search.

Flow:
  1. Fetch all pages accessible to the integration via Notion search API
  2. Extract plain text from each page's block tree (recursive)
  3. Chunk text into ~1000-char segments with 10% (100-char) overlap
  4. Generate embeddings via OpenAI text-embedding-3-small (dim=1536)
  5. Upsert into kb_documents and kb_document_chunks Supabase tables

Usage:
    python sync_knowledge_base.py

Required environment variables:
    NOTION_API_KEY   — Notion integration token (starts with secret_...)
    OPENAI_API_KEY   — OpenAI API key
    SUPABASE_URL     — Supabase project URL
    SUPABASE_KEY     — Supabase service role key

Supabase tables expected:
    kb_documents (
        id             TEXT PRIMARY KEY,
        notion_page_id TEXT UNIQUE NOT NULL,
        title          TEXT,
        url            TEXT,
        content        TEXT,
        created_at     TIMESTAMPTZ DEFAULT now()
    )
    kb_document_chunks (
        id             TEXT PRIMARY KEY,
        document_id    TEXT REFERENCES kb_documents(id),
        chunk_index    INTEGER NOT NULL,
        content        TEXT NOT NULL,
        embedding      vector(1536)
    )

    For semantic search, create this Postgres function in Supabase:
        CREATE OR REPLACE FUNCTION match_kb_document_chunks(
            query_embedding vector(1536),
            match_count     INT DEFAULT 5
        )
        RETURNS TABLE (
            id          TEXT,
            document_id TEXT,
            chunk_index INT,
            content     TEXT,
            similarity  FLOAT
        )
        LANGUAGE SQL STABLE AS $$
            SELECT
                c.id,
                c.document_id,
                c.chunk_index,
                c.content,
                1 - (c.embedding <=> query_embedding) AS similarity
            FROM kb_document_chunks c
            ORDER BY c.embedding <=> query_embedding
            LIMIT match_count;
        $$;
"""

import os
import time
import hashlib
from dotenv import load_dotenv
from notion_client import Client as NotionClient
from openai import OpenAI
from services.supabase_client import _get_client, _reset_client

load_dotenv()

# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

def _get_notion_client() -> NotionClient:
    api_key = os.environ.get("NOTION_API_KEY")
    if not api_key:
        raise RuntimeError("Environment variable 'NOTION_API_KEY' is not set.")
    return NotionClient(auth=api_key)


def _get_openai_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Environment variable 'OPENAI_API_KEY' is not set.")
    return OpenAI(api_key=api_key)


# ---------------------------------------------------------------------------
# Notion text extraction
# ---------------------------------------------------------------------------

# Block types whose rich_text array contains readable content
_BLOCK_TEXT_TYPES = {
    "paragraph",
    "heading_1",
    "heading_2",
    "heading_3",
    "bulleted_list_item",
    "numbered_list_item",
    "toggle",
    "quote",
    "callout",
    "code",
}


def _extract_rich_text(rich_text_list: list) -> str:
    """Flatten a Notion rich_text array into a plain string."""
    return "".join(t.get("plain_text", "") for t in rich_text_list)


def _extract_block_text(block: dict) -> str:
    """Extract all plain text from a single Notion block."""
    btype = block.get("type", "")
    if btype not in _BLOCK_TEXT_TYPES:
        return ""
    inner = block.get(btype, {})
    return _extract_rich_text(inner.get("rich_text", []))


def _fetch_page_text(notion: NotionClient, page_id: str) -> str:
    """
    Recursively fetch all text content from a Notion page's block tree.
    Returns a single newline-separated string of non-empty text blocks.
    """
    lines: list[str] = []

    def _collect(block_id: str) -> None:
        cursor = None
        while True:
            kwargs: dict = {"block_id": block_id, "page_size": 100}
            if cursor:
                kwargs["start_cursor"] = cursor
            resp = notion.blocks.children.list(**kwargs)
            for block in resp.get("results", []):
                text = _extract_block_text(block)
                if text.strip():
                    lines.append(text.strip())
                if block.get("has_children"):
                    _collect(block["id"])
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")

    _collect(page_id)
    return "\n".join(lines)


def _fetch_all_notion_pages(notion: NotionClient) -> list[dict]:
    """
    Return all pages accessible to the integration via Notion search.
    Each item: {page_id, title, url}
    """
    pages: list[dict] = []
    cursor = None

    while True:
        kwargs: dict = {
            "filter":    {"value": "page", "property": "object"},
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.search(**kwargs)

        for result in resp.get("results", []):
            page_id = result["id"]
            url     = result.get("url", "")

            # Title lives inside a title-type property
            title = ""
            for prop in result.get("properties", {}).values():
                if prop.get("type") == "title":
                    title = _extract_rich_text(prop.get("title", []))
                    break

            # Fallback for pages whose top-level object has a title array
            if not title:
                top_title = result.get("title", [])
                if isinstance(top_title, list):
                    title = _extract_rich_text(top_title)

            pages.append({
                "page_id": page_id,
                "title":   title or page_id,
                "url":     url,
            })

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return pages


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

_CHUNK_SIZE    = 1500   # larger chunks keep table rows intact
_CHUNK_OVERLAP = 200    # ~13% overlap for cross-chunk context


def _chunk_text(
    text: str,
    chunk_size: int = _CHUNK_SIZE,
    overlap:    int = _CHUNK_OVERLAP,
) -> list[str]:
    """
    Split text into overlapping chunks of approximately chunk_size characters.
    Breaks on the nearest whitespace boundary to avoid splitting mid-word.
    Adjacent chunks share `overlap` characters of context.
    """
    if not text.strip():
        return []

    chunks: list[str] = []
    start  = 0
    length = len(text)

    while start < length:
        end = start + chunk_size

        if end >= length:
            # Final chunk — take everything remaining
            chunk = text[start:].strip()
            if chunk:
                chunks.append(chunk)
            break

        # Walk back to the nearest whitespace so we don't split mid-word
        boundary = text.rfind(" ", start, end)
        if boundary == -1 or boundary <= start:
            boundary = end   # No whitespace found — hard cut

        chunk = text[start:boundary].strip()
        if chunk:
            chunks.append(chunk)

        start = boundary - overlap

    return chunks


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------

_EMBEDDING_MODEL = "text-embedding-3-small"
_BATCH_SIZE      = 100    # Max texts per OpenAI embeddings call


def _embed_texts(openai_client: OpenAI, texts: list[str]) -> list[list[float]]:
    """
    Generate embeddings for a list of texts using text-embedding-3-small.
    Batches up to _BATCH_SIZE texts per API call to stay within limits.
    Returns embeddings in the same order as the input.
    """
    all_embeddings: list[list[float]] = []

    for i in range(0, len(texts), _BATCH_SIZE):
        batch = texts[i : i + _BATCH_SIZE]
        resp  = openai_client.embeddings.create(model=_EMBEDDING_MODEL, input=batch)
        # Response items are returned with an index — sort to preserve order
        sorted_items = sorted(resp.data, key=lambda x: x.index)
        all_embeddings.extend(item.embedding for item in sorted_items)

    return all_embeddings


# ---------------------------------------------------------------------------
# Stable deterministic IDs
# ---------------------------------------------------------------------------

def _document_id(notion_page_id: str) -> str:
    """Deterministic 32-char ID for a kb_documents row."""
    return hashlib.sha256(notion_page_id.encode()).hexdigest()[:32]


def _chunk_id(notion_page_id: str, chunk_index: int) -> str:
    """Deterministic 32-char ID for a kb_document_chunks row."""
    key = f"{notion_page_id}:{chunk_index}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Supabase upsert helpers
# ---------------------------------------------------------------------------

def _upsert_document(page_id: str, title: str, url: str, full_text: str) -> str:
    """
    Upsert a row into kb_documents. Returns the document's stable ID.
    Conflicts are resolved on the notion_page_id unique column.
    """
    doc_id = _document_id(page_id)
    record = {
        "id":             doc_id,
        "notion_page_id": page_id,
        "title":          title,
        "url":            url,
        "content":        full_text,
    }
    _supabase_call(lambda: (
        _get_client()
        .table("kb_documents")
        .upsert(record, on_conflict="notion_page_id")
        .execute()
    ))
    return doc_id


_UPSERT_RETRIES = [2, 4, 8, 16]   # backoff seconds between retries


def _is_retryable(exc: Exception) -> bool:
    """Return True if the exception looks like a transient network/DB failure."""
    import errno, socket
    # Windows connection-reset (WinError 10054) and similar socket errors
    if isinstance(exc, OSError) and exc.errno in (
        errno.ECONNRESET, errno.ECONNABORTED, errno.ECONNREFUSED,
        errno.EPIPE, errno.ETIMEDOUT, 10054, 10053, 10061,
    ):
        return True
    msg = str(exc).lower()
    triggers = [
        "57014",                  # PostgreSQL statement_timeout
        "502",                    # Gateway / Cloudflare timeout
        "statement timeout",
        "canceling statement",
        "connectionterminated",   # HTTP/2 stream limit
        "10054",                  # WinError connection reset
        "forcibly closed",        # WinError 10054 text
        "remote end closed",
        "broken pipe",
        "eof occurred",
        "connection reset",
        "connection was closed",
        "connection aborted",
    ]
    return any(t in msg for t in triggers)


def _supabase_call(fn, *args, **kwargs):
    """
    Call any Supabase table operation with retry + fresh-client logic.
    fn should be a callable that returns a Supabase query builder.
    """
    last_exc = None
    for attempt, delay in enumerate([0] + _UPSERT_RETRIES):
        if delay:
            print(f"[kb-sync]     retrying in {delay}s (attempt {attempt+1})...", flush=True)
            time.sleep(delay)
        try:
            return fn()
        except Exception as exc:
            if _is_retryable(exc):
                last_exc = exc
                try:
                    _reset_client()
                except Exception:
                    pass
                if attempt < len(_UPSERT_RETRIES):
                    continue
                raise RuntimeError(f"Exhausted retries after {len(_UPSERT_RETRIES)} attempts: {exc}") from exc
            raise   # non-retryable — propagate immediately
    raise last_exc


def _upsert_chunks(
    doc_id:     str,
    page_id:    str,
    chunks:     list[str],
    embeddings: list[list[float]],
) -> None:
    """
    Upsert all chunks for a document into kb_document_chunks one row at a time.
    Each row goes through _supabase_call for retry + connection-reset logic.
    Conflicts are resolved on id (stable hash of page_id + chunk_index).
    """
    if not chunks:
        return

    for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        record = {
            "id":          _chunk_id(page_id, i),
            "document_id": doc_id,
            "chunk_index": i,
            "content":     chunk,
            "embedding":   embedding,
        }
        _supabase_call(lambda r=record: (
            _get_client()
            .table("kb_document_chunks")
            .upsert(r, on_conflict="id")
            .execute()
        ))


# ---------------------------------------------------------------------------
# Main sync loop
# ---------------------------------------------------------------------------

def sync() -> dict:
    """
    Full sync: fetch all Notion pages → chunk text → embed → upsert to Supabase.
    Returns a summary dict with page and chunk counts.
    """
    notion        = _get_notion_client()
    openai_client = _get_openai_client()

    print("[kb-sync] Fetching pages from Notion...", flush=True)
    pages = _fetch_all_notion_pages(notion)
    print(f"[kb-sync] Found {len(pages)} pages.", flush=True)

    total_chunks    = 0
    pages_processed = 0
    pages_skipped   = 0

    for page in pages:
        page_id = page["page_id"]
        title   = page["title"]
        url     = page["url"]

        print(f"[kb-sync] Processing: {title!r} ({page_id})", flush=True)

        try:
            text = _fetch_page_text(notion, page_id)
        except Exception as exc:
            print(f"[kb-sync]   SKIP: could not fetch blocks — {exc}", flush=True)
            pages_skipped += 1
            continue

        if not text.strip():
            print("[kb-sync]   SKIP: page has no text content.", flush=True)
            pages_skipped += 1
            continue

        chunks = _chunk_text(text)
        if not chunks:
            print("[kb-sync]   SKIP: chunking produced no output.", flush=True)
            pages_skipped += 1
            continue

        print(f"[kb-sync]   {len(chunks)} chunk(s) — generating embeddings...", flush=True)

        try:
            embeddings = _embed_texts(openai_client, chunks)
        except Exception as exc:
            print(f"[kb-sync]   SKIP: embedding error — {exc}", flush=True)
            pages_skipped += 1
            continue

        try:
            doc_id = _upsert_document(page_id, title, url, text)
            _upsert_chunks(doc_id, page_id, chunks, embeddings)
        except Exception as exc:
            print(f"[kb-sync]   SKIP: Supabase upsert error — {exc}", flush=True)
            pages_skipped += 1
            continue

        pages_processed += 1
        total_chunks    += len(chunks)
        print(f"[kb-sync]   Done. {len(chunks)} chunk(s) upserted.", flush=True)

        # Brief pause to stay within Notion and OpenAI rate limits
        time.sleep(0.5)

    summary = {
        "pages_found":     len(pages),
        "pages_processed": pages_processed,
        "pages_skipped":   pages_skipped,
        "total_chunks":    total_chunks,
    }
    print(f"[kb-sync] Sync complete: {summary}", flush=True)
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sync Notion pages into Supabase kb tables.")
    parser.add_argument(
        "--full-resync",
        action="store_true",
        help="Delete all existing chunks and documents before syncing (rebuilds from scratch).",
    )
    args = parser.parse_args()

    if args.full_resync:
        print("[kb-sync] FULL RESYNC — deleting all existing chunks and documents...", flush=True)
        try:
            sb = _get_client()
            # TRUNCATE via RPC — bulk DELETE times out on large tables.
            # Requires this function in Supabase (run once in SQL editor):
            #
            #   CREATE OR REPLACE FUNCTION truncate_kb_tables()
            #   RETURNS void LANGUAGE SQL SECURITY DEFINER AS $$
            #     TRUNCATE kb_document_chunks;
            #     TRUNCATE kb_documents CASCADE;
            #   $$;
            sb.rpc("truncate_kb_tables", {}).execute()
            print("[kb-sync]   kb_document_chunks + kb_documents truncated.", flush=True)
        except Exception as exc:
            print(f"[kb-sync]   ERROR during delete: {exc}", flush=True)
            raise SystemExit(1)

    result = sync()
    print(result)
