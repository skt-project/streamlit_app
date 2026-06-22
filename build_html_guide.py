"""Converts a User_Guide markdown file into styled HTML.

Usage: python build_html_guide.py User_Guide.md User_Guide.html
"""
import sys
import markdown

TEMPLATE = """<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Inter, Arial, sans-serif; max-width: 960px; margin: 40px auto; padding: 0 24px; color: #1C1C1C; line-height: 1.6; }}
  h1 {{ color: #14506A; border-bottom: 3px solid #1E6B8A; padding-bottom: 10px; }}
  h2 {{ color: #1E6B8A; margin-top: 48px; border-bottom: 1px solid #BEE3EE; padding-bottom: 6px; }}
  h3 {{ color: #14506A; }}
  img {{ max-width: 100%; border: 1px solid #BEE3EE; border-radius: 8px; margin: 12px 0; box-shadow: 0 2px 8px rgba(30,107,138,0.10); }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ border: 1px solid #D0D8E4; padding: 8px 12px; text-align: left; }}
  th {{ background: #EAF6FB; color: #14506A; }}
  code {{ background: #F4FAFC; padding: 2px 6px; border-radius: 4px; }}
  hr {{ border: none; border-top: 1px solid #BEE3EE; margin: 32px 0; }}
  a {{ color: #1E6B8A; }}
  ol li {{ margin-bottom: 4px; }}
</style>
</head>
<body>
{body}
</body>
</html>
"""


def build(md_path, html_path, lang="en", title="Distributor Operational Assessment — User Guide"):
    with open(md_path, encoding="utf-8") as f:
        md_text = f.read()
    body = markdown.markdown(md_text, extensions=["tables", "toc", "fenced_code"])
    html = TEMPLATE.format(lang=lang, title=title, body=body)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Generated {html_path} ({len(html)} bytes)")


if __name__ == "__main__":
    md_path = sys.argv[1]
    html_path = sys.argv[2]
    lang = sys.argv[3] if len(sys.argv) > 3 else "en"
    title = sys.argv[4] if len(sys.argv) > 4 else "Distributor Operational Assessment — User Guide"
    build(md_path, html_path, lang, title)
