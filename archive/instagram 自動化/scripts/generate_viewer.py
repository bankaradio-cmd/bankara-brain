from __future__ import annotations

import csv
import html
import re
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "viewer" / "index.html"


SECTIONS = [
    (
        "実行用ドキュメント",
        [
            "docs/instagram-first-account-strategy-ja.md",
            "docs/instagram-first-offer-design-ja.md",
            "docs/instagram-first-20-content-ideas-ja.md",
            "docs/instagram-first-5-reel-scripts-ja.md",
            "docs/instagram-profile-copy-ja.md",
        ],
    ),
    (
        "設計とリサーチ",
        [
            "docs/instagram-money-engine-design-ja.md",
            "docs/instagram-niche-scorecard-ja.md",
            "docs/instagram-monetization-research.md",
            "docs/instagram-next-step-execution-plan-ja.md",
            "docs/instagram-ai-pdca-blueprint.md",
        ],
    ),
    (
        "無料オファー7点セット",
        [
            "offers/free-template-7set/README.md",
            "offers/free-template-7set/01_reel_script_template.md",
            "offers/free-template-7set/02_hook_examples.csv",
            "offers/free-template-7set/03_cta_templates.csv",
            "offers/free-template-7set/04_story_ideas.csv",
            "offers/free-template-7set/05_post_calendar.csv",
            "offers/free-template-7set/06_ai_prompts.md",
            "offers/free-template-7set/07_improvement_checklist.md",
        ],
    ),
]


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "section"


def parse_inline(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", escaped)
    return escaped


def wrap_paragraph(lines: list[str]) -> str:
    content = " ".join(line.strip() for line in lines if line.strip())
    if not content:
        return ""
    return f"<p>{parse_inline(content)}</p>"


def markdown_to_html(text: str) -> str:
    lines = text.splitlines()
    parts: list[str] = []
    paragraph: list[str] = []
    bullet_items: list[str] = []
    ordered_items: list[str] = []
    in_code = False
    code_lines: list[str] = []
    code_lang = ""
    i = 0

    def flush_paragraph() -> None:
        nonlocal paragraph
        block = wrap_paragraph(paragraph)
        if block:
            parts.append(block)
        paragraph = []

    def flush_bullets() -> None:
        nonlocal bullet_items
        if bullet_items:
            items = "".join(f"<li>{parse_inline(item)}</li>" for item in bullet_items)
            parts.append(f"<ul>{items}</ul>")
        bullet_items = []

    def flush_ordered() -> None:
        nonlocal ordered_items
        if ordered_items:
            items = "".join(f"<li>{parse_inline(item)}</li>" for item in ordered_items)
            parts.append(f"<ol>{items}</ol>")
        ordered_items = []

    while i < len(lines):
        line = lines[i]

        if line.startswith("```"):
            flush_paragraph()
            flush_bullets()
            flush_ordered()
            if in_code:
                code_html = html.escape("\n".join(code_lines))
                class_attr = f' class="language-{code_lang}"' if code_lang else ""
                parts.append(f"<pre><code{class_attr}>{code_html}</code></pre>")
                code_lines = []
                code_lang = ""
                in_code = False
            else:
                in_code = True
                code_lang = line[3:].strip()
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        if not line.strip():
            flush_paragraph()
            flush_bullets()
            flush_ordered()
            i += 1
            continue

        if "|" in line and line.strip().startswith("|") and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if next_line.startswith("|") and set(next_line.replace("|", "").replace("-", "").replace(":", "").strip()) == set():
                flush_paragraph()
                flush_bullets()
                flush_ordered()
                table_lines = [line.strip()]
                i += 2
                while i < len(lines) and lines[i].strip().startswith("|"):
                    table_lines.append(lines[i].strip())
                    i += 1
                headers = [parse_inline(cell.strip()) for cell in table_lines[0].strip("|").split("|")]
                rows = []
                for row_line in table_lines[1:]:
                    cells = [parse_inline(cell.strip()) for cell in row_line.strip("|").split("|")]
                    row_html = "".join(f"<td>{cell}</td>" for cell in cells)
                    rows.append(f"<tr>{row_html}</tr>")
                header_html = "".join(f"<th>{cell}</th>" for cell in headers)
                parts.append(
                    "<div class=\"table-wrap\"><table>"
                    f"<thead><tr>{header_html}</tr></thead>"
                    f"<tbody>{''.join(rows)}</tbody>"
                    "</table></div>"
                )
                continue

        heading_match = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading_match:
            flush_paragraph()
            flush_bullets()
            flush_ordered()
            level = len(heading_match.group(1)) + 1
            title = parse_inline(heading_match.group(2).strip())
            parts.append(f"<h{level}>{title}</h{level}>")
            i += 1
            continue

        bullet_match = re.match(r"^-\s+(.*)$", line)
        if bullet_match:
            flush_paragraph()
            flush_ordered()
            bullet_items.append(bullet_match.group(1).strip())
            i += 1
            continue

        ordered_match = re.match(r"^\d+\.\s+(.*)$", line)
        if ordered_match:
            flush_paragraph()
            flush_bullets()
            ordered_items.append(ordered_match.group(1).strip())
            i += 1
            continue

        paragraph.append(line)
        i += 1

    flush_paragraph()
    flush_bullets()
    flush_ordered()
    return "\n".join(parts)


def csv_to_html(text: str) -> str:
    rows = list(csv.reader(text.splitlines()))
    if not rows:
        return "<p>Empty CSV</p>"

    header = rows[0]
    body = rows[1:]
    header_html = "".join(f"<th>{html.escape(cell)}</th>" for cell in header)
    body_html = []
    for row in body:
        row += [""] * (len(header) - len(row))
        cells = "".join(f"<td>{html.escape(cell)}</td>" for cell in row[: len(header)])
        body_html.append(f"<tr>{cells}</tr>")
    return (
        "<div class=\"table-wrap\"><table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_html)}</tbody>"
        "</table></div>"
    )


def title_from_path(path: str) -> str:
    stem = Path(path).stem
    return stem.replace("-", " ")


def render_file_card(rel_path: str) -> tuple[str, str]:
    file_path = ROOT / rel_path
    text = file_path.read_text(encoding="utf-8")
    anchor = slugify(rel_path)
    ext = file_path.suffix.lower()
    if ext == ".md":
        body = markdown_to_html(text)
    elif ext == ".csv":
        body = csv_to_html(text)
    else:
        body = f"<pre>{html.escape(text)}</pre>"

    card = f"""
    <article class="doc-card" id="{anchor}">
      <div class="doc-meta">
        <span class="doc-type">{ext[1:].upper() or 'FILE'}</span>
        <span class="doc-path">{html.escape(rel_path)}</span>
      </div>
      <div class="doc-body">
        {body}
      </div>
    </article>
    """
    return anchor, card


def build_nav() -> str:
    groups = []
    for section_name, files in SECTIONS:
        items = []
        for rel_path in files:
            anchor = slugify(rel_path)
            items.append(
                f'<li><a href="#{anchor}">{html.escape(title_from_path(rel_path))}</a></li>'
            )
        groups.append(
            f"""
            <section class="nav-group">
              <h3>{html.escape(section_name)}</h3>
              <ul>{''.join(items)}</ul>
            </section>
            """
        )
    return "\n".join(groups)


def build_sections() -> str:
    sections_html = []
    for section_name, files in SECTIONS:
        cards = []
        for rel_path in files:
            _, card = render_file_card(rel_path)
            cards.append(card)
        sections_html.append(
            f"""
            <section class="content-group">
              <div class="group-heading">
                <h2>{html.escape(section_name)}</h2>
                <p>{len(files)} files</p>
              </div>
              {''.join(cards)}
            </section>
            """
        )
    return "\n".join(sections_html)


def build_html() -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    nav_html = build_nav()
    sections_html = build_sections()
    return f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Instagram Automation Viewer</title>
  <style>
    :root {{
      --bg: #f5efe6;
      --panel: #fffdf9;
      --line: #e7d8c6;
      --text: #1f1a17;
      --muted: #6a5f56;
      --accent: #b24c1a;
      --accent-soft: #f4ded1;
      --code: #f3f0eb;
      --shadow: 0 12px 36px rgba(56, 35, 18, 0.08);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      background:
        radial-gradient(circle at top right, rgba(178, 76, 26, 0.12), transparent 28%),
        linear-gradient(180deg, #f8f2ea 0%, var(--bg) 100%);
      color: var(--text);
      font-family: "Hiragino Sans", "Noto Sans JP", sans-serif;
      line-height: 1.7;
    }}

    a {{
      color: inherit;
      text-decoration: none;
    }}

    .layout {{
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr);
      min-height: 100vh;
    }}

    .sidebar {{
      position: sticky;
      top: 0;
      align-self: start;
      height: 100vh;
      padding: 28px 22px;
      border-right: 1px solid var(--line);
      background: rgba(255, 253, 249, 0.84);
      backdrop-filter: blur(10px);
      overflow-y: auto;
    }}

    .brand {{
      margin-bottom: 22px;
    }}

    .brand h1 {{
      margin: 0 0 8px;
      font-size: 24px;
      line-height: 1.2;
    }}

    .brand p {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }}

    .nav-group + .nav-group {{
      margin-top: 22px;
    }}

    .nav-group h3 {{
      margin: 0 0 10px;
      font-size: 13px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--accent);
    }}

    .nav-group ul {{
      list-style: none;
      margin: 0;
      padding: 0;
      display: grid;
      gap: 8px;
    }}

    .nav-group a {{
      display: block;
      padding: 10px 12px;
      border: 1px solid transparent;
      border-radius: 12px;
      color: var(--muted);
      transition: 0.18s ease;
    }}

    .nav-group a:hover {{
      background: var(--accent-soft);
      border-color: var(--line);
      color: var(--text);
    }}

    .main {{
      padding: 34px;
    }}

    .hero {{
      margin-bottom: 28px;
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: linear-gradient(135deg, rgba(255, 253, 249, 0.98), rgba(255, 245, 237, 0.92));
      box-shadow: var(--shadow);
    }}

    .hero .eyebrow {{
      display: inline-block;
      margin-bottom: 10px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}

    .hero h2 {{
      margin: 0 0 10px;
      font-size: clamp(28px, 4vw, 44px);
      line-height: 1.1;
    }}

    .hero p {{
      margin: 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 16px;
    }}

    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-top: 22px;
    }}

    .stat {{
      padding: 16px 18px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.6);
    }}

    .stat strong {{
      display: block;
      font-size: 24px;
      line-height: 1;
      margin-bottom: 6px;
    }}

    .stat span {{
      color: var(--muted);
      font-size: 13px;
    }}

    .content-group + .content-group {{
      margin-top: 34px;
    }}

    .group-heading {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
    }}

    .group-heading h2 {{
      margin: 0;
      font-size: 26px;
    }}

    .group-heading p {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }}

    .doc-card {{
      padding: 26px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }}

    .doc-card + .doc-card {{
      margin-top: 18px;
    }}

    .doc-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-bottom: 18px;
      color: var(--muted);
      font-size: 13px;
    }}

    .doc-type {{
      padding: 5px 9px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 700;
    }}

    .doc-path {{
      font-family: ui-monospace, "SFMono-Regular", monospace;
      overflow-wrap: anywhere;
    }}

    .doc-body h2,
    .doc-body h3,
    .doc-body h4,
    .doc-body h5,
    .doc-body h6 {{
      margin-top: 1.5em;
      margin-bottom: 0.5em;
      line-height: 1.25;
    }}

    .doc-body h2 {{
      font-size: 28px;
    }}

    .doc-body h3 {{
      font-size: 22px;
    }}

    .doc-body h4 {{
      font-size: 18px;
    }}

    .doc-body p,
    .doc-body li {{
      color: var(--text);
    }}

    .doc-body p {{
      margin: 0 0 1em;
    }}

    .doc-body ul,
    .doc-body ol {{
      margin: 0 0 1.1em 1.25em;
    }}

    .doc-body code {{
      padding: 0.15em 0.4em;
      border-radius: 6px;
      background: var(--code);
      font-family: ui-monospace, "SFMono-Regular", monospace;
      font-size: 0.92em;
    }}

    .doc-body pre {{
      margin: 0 0 1.2em;
      padding: 16px;
      overflow-x: auto;
      border-radius: 16px;
      background: #221d19;
      color: #fdf4ea;
      font-size: 14px;
    }}

    .doc-body pre code {{
      padding: 0;
      background: transparent;
      color: inherit;
    }}

    .table-wrap {{
      margin: 0 0 1.25em;
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 16px;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 640px;
      background: #fff;
    }}

    th,
    td {{
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}

    th {{
      background: #fbf1e7;
      font-size: 13px;
      letter-spacing: 0.02em;
      color: var(--accent);
    }}

    tr:last-child td {{
      border-bottom: none;
    }}

    @media (max-width: 980px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}

      .sidebar {{
        position: relative;
        height: auto;
        border-right: none;
        border-bottom: 1px solid var(--line);
      }}

      .main {{
        padding: 20px;
      }}

      .hero {{
        padding: 22px;
      }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">
        <h1>Instagram 自動化 Viewer</h1>
        <p>現在の設計書、配布テンプレ、初期台本を1か所で読めるようにまとめた静的ビューアです。</p>
      </div>
      {nav_html}
    </aside>
    <main class="main">
      <section class="hero">
        <span class="eyebrow">Workspace Viewer</span>
        <h2>今やっていることを、そのまま読める形にまとめました。</h2>
        <p>
          Instagram自動化プロジェクトの戦略、収益設計、初期コンテンツ、無料オファー素材を、
          ブラウザで普通に確認できるように整理しています。生成日時は {generated_at} です。
        </p>
        <div class="stats">
          <div class="stat"><strong>18</strong><span>表示対象ファイル</span></div>
          <div class="stat"><strong>3</strong><span>主要カテゴリ</span></div>
          <div class="stat"><strong>5</strong><span>初期リール台本</span></div>
          <div class="stat"><strong>7</strong><span>無料配布テンプレ</span></div>
        </div>
      </section>
      {sections_html}
    </main>
  </div>
</body>
</html>
"""


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(build_html(), encoding="utf-8")
    print(f"Generated {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
