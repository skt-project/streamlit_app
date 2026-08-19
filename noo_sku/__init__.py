"""NOO & SKU Mapping — shared logic for the distributor upload portal.

Layout:
    config.py         verified spreadsheet IDs, tabs, column positions, settings
    normalize.py      trimming, hashing, business-timezone dates
    customer_code.py  brand prefix + distributor abbreviation resolution
    parsers.py        reads BD Support's real .xlsx templates
    validators.py     row-level business rules
    duplicates.py     identity/content classification
    writer.py         pool row construction + guarded append
    sources.py        Sheets / Drive / BigQuery readers

Everything except `sources.py` is pure and unit-tested without credentials.
"""

__all__ = [
    "config", "normalize", "customer_code", "parsers", "validators",
    "duplicates", "writer", "sources",
]
