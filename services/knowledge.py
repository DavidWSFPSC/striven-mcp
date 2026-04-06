"""
Knowledge base service for Ask WilliamSmith.

Loads markdown knowledge files at startup and provides:
  1. Full-context retrieval — all files returned as a formatted block
     for inclusion in the system prompt.
  2. Keyword search — returns the most relevant sections for a query,
     used by the `search_knowledge` Claude tool.

Architecture note:
  This is a structured-file RAG approach. Knowledge lives in /knowledge/*.md
  files versioned with the code. Each file covers one domain. Sections within
  files are delimited by "## " headings. At query time, sections are scored by
  keyword overlap with the query and the top-K are returned.

  This is the right approach for stable, curated content (audit rules,
  procedures, field definitions). For large uploaded documents (pricing
  guides, spec sheets), extend with a vector DB layer later.
"""

import os
import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge"

# Files always included in system prompt context (small, critical)
_ALWAYS_LOAD = [
    "audit_rules.md",
    "order_lifecycle.md",
    "product_categories.md",
]

# Files available via search tool (loaded but not always in context)
_SEARCHABLE = [
    "audit_rules.md",
    "order_lifecycle.md",
    "product_categories.md",
    "customer_types.md",
    "striven_fields.md",
    "order_naming.md",
    "roles.md",
    "company_overview.md",
    "estimating_standards.md",
]


# ---------------------------------------------------------------------------
# Internal loading
# ---------------------------------------------------------------------------

def _load_file(filename: str) -> str | None:
    """Read a single knowledge file. Returns None if the file does not exist."""
    path = _KNOWLEDGE_DIR / filename
    if not path.exists():
        print(f"[knowledge] WARNING: {path} not found — skipping.", flush=True)
        return None
    try:
        text = path.read_text(encoding="utf-8")
        print(f"[knowledge] Loaded {filename} ({len(text)} chars)", flush=True)
        return text
    except Exception as exc:
        print(f"[knowledge] ERROR loading {filename}: {exc}", flush=True)
        return None


def _split_sections(text: str, filename: str) -> list[dict]:
    """
    Split a markdown file into sections at '## ' headings.
    Returns a list of dicts: {filename, heading, content, full_text}
    The first section (before any '## ' heading) is kept if non-empty.
    """
    sections: list[dict] = []
    parts = re.split(r"(?m)^(## .+)$", text)

    # parts alternates: [pre-heading-text, heading, body, heading, body, ...]
    # If no headings, parts = [full_text]

    if len(parts) == 1:
        # No ## headings — treat entire file as one section
        sections.append({
            "filename": filename,
            "heading":  filename.replace(".md", "").replace("_", " ").title(),
            "content":  parts[0].strip(),
            "full_text": parts[0].strip(),
        })
        return sections

    # Preamble (before first heading)
    preamble = parts[0].strip()
    if preamble:
        sections.append({
            "filename": filename,
            "heading":  filename.replace(".md", "").replace("_", " ").title(),
            "content":  preamble,
            "full_text": preamble,
        })

    # Heading + body pairs
    for i in range(1, len(parts) - 1, 2):
        heading = parts[i].lstrip("#").strip()
        body    = parts[i + 1].strip() if i + 1 < len(parts) else ""
        full    = f"{parts[i]}\n{body}"
        sections.append({
            "filename": filename,
            "heading":  heading,
            "content":  body,
            "full_text": full,
        })

    return sections


# ---------------------------------------------------------------------------
# Startup load
# ---------------------------------------------------------------------------

_all_sections: list[dict] = []       # Every section from every searchable file
_always_context: str       = ""      # Pre-built block for system prompt


def load_all() -> None:
    """
    Load all knowledge files into memory.
    Call once at application startup.
    """
    global _all_sections, _always_context

    _all_sections = []

    for filename in _SEARCHABLE:
        text = _load_file(filename)
        if text:
            _all_sections.extend(_split_sections(text, filename))

    print(
        f"[knowledge] Loaded {len(_SEARCHABLE)} files → "
        f"{len(_all_sections)} sections total",
        flush=True,
    )

    # Build the always-on context block (audit rules + lifecycle + products)
    always_parts: list[str] = []
    for filename in _ALWAYS_LOAD:
        text = _load_file(filename)
        if text:
            label = filename.replace(".md", "").replace("_", " ").upper()
            always_parts.append(f"=== KNOWLEDGE: {label} ===\n{text.strip()}")

    _always_context = "\n\n".join(always_parts)
    print(
        f"[knowledge] Always-on context block: {len(_always_context)} chars",
        flush=True,
    )


def get_always_context() -> str:
    """
    Return the pre-built always-on knowledge block for inclusion in the
    system prompt. Contains audit rules, order lifecycle, and product categories.
    """
    return _always_context


# ---------------------------------------------------------------------------
# Keyword search
# ---------------------------------------------------------------------------

def _score_section(section: dict, query_tokens: set[str]) -> int:
    """
    Score a section by counting how many query tokens appear in it.
    Heading matches count double.
    """
    heading_tokens = set(section["heading"].lower().split())
    content_tokens = set(re.findall(r"\w+", section["content"].lower()))

    heading_matches = len(query_tokens & heading_tokens)
    content_matches = len(query_tokens & content_tokens)

    return heading_matches * 2 + content_matches


def search(query: str, top_k: int = 5, min_score: int = 1) -> list[dict]:
    """
    Search all knowledge sections by keyword overlap with the query.

    Returns a list of up to top_k matching sections, sorted by relevance score.
    Each element: {filename, heading, content, full_text, score}

    Args:
        query:     The user's question or search string.
        top_k:     Maximum number of sections to return (default 5).
        min_score: Minimum score to include a section (default 1 — must match
                   at least one token).

    Returns:
        Empty list if nothing matches above min_score.
    """
    if not _all_sections:
        print("[knowledge] WARNING: search called before load_all()", flush=True)
        return []

    # Tokenise query — remove stop words to reduce noise
    STOP = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been",
        "have", "has", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "shall", "can", "need", "must",
        "for", "of", "in", "on", "at", "to", "from", "by", "with",
        "about", "into", "and", "or", "but", "not", "no", "yes",
        "what", "which", "who", "how", "when", "where", "why",
        "i", "me", "my", "we", "our", "you", "your", "it", "its",
        "this", "that", "these", "those", "there", "here",
    }
    query_tokens = {
        t.lower() for t in re.findall(r"\w+", query)
        if t.lower() not in STOP and len(t) > 2
    }

    if not query_tokens:
        return []

    scored = [
        {**sec, "score": _score_section(sec, query_tokens)}
        for sec in _all_sections
    ]
    scored = [s for s in scored if s["score"] >= min_score]
    scored.sort(key=lambda s: s["score"], reverse=True)

    results = scored[:top_k]
    print(
        f"[knowledge] search({query!r}) → "
        f"{len(results)} results (top score={results[0]['score'] if results else 0})",
        flush=True,
    )
    return results


def format_search_results(results: list[dict]) -> str:
    """
    Format search results as a readable string for returning to Claude
    as a tool result.
    """
    if not results:
        return "No relevant knowledge found for this query."

    parts: list[str] = []
    for r in results:
        source = f"{r['filename']} › {r['heading']}"
        parts.append(f"**{source}** (relevance: {r['score']})\n\n{r['full_text']}")

    return "\n\n---\n\n".join(parts)
