#!/usr/bin/env bash
# Regenerate HTML documentation from Markdown using MDWeb.
# Requires MDWeb built at ../MDWeb (or set MDWEB_BIN).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS="$ROOT/docs"
MDWEB_BIN="${MDWEB_BIN:-$ROOT/../MDWeb/src/MDWeb.Cli/bin/Debug/net10.0/mdweb}"
THEME="${MDWEB_THEME:-$ROOT/../MDWeb/themes/default}"
STAGING="$(mktemp -d)"

cleanup() { rm -rf "$STAGING"; }
trap cleanup EXIT

cp "$DOCS"/*.md "$STAGING/"

"$MDWEB_BIN" \
  --source "$STAGING" \
  --output "$STAGING/out" \
  --theme "$THEME" \
  --title "RainDB" \
  --description "Embedded columnar OLAP database engine for .NET" \
  --footer '<p><a href="index.html">RainDB home</a> · <a href="https://github.com/hoihky/RainDB" target="_blank" rel="noopener">GitHub</a> · MIT License</p>' \
  --no-fix-markdown-links

cp "$STAGING/out"/*.html "$DOCS/"
cp -R "$STAGING/out/assets/"* "$DOCS/assets/"
cp "$ROOT/../MDWeb/docs/assets/css/site.css" "$DOCS/assets/css/site.css"

for f in Programming-Guide.html RainDB-Internals.html Implementation-Status.html Development-Roadmap.html; do
  perl -i -0pe 's|<a href="Development-Roadmap\.html" class="brand">RainDB</a>|<a href="index.html" class="brand">RainDB</a>\n      <nav class="topbar-nav" aria-label="Primary">\n        <a href="index.html">Overview</a>\n        <a href="Programming-Guide.html">Programming Guide</a>\n        <a href="RainDB-Internals.html">Internals</a>\n        <a href="https://github.com/hoihky/RainDB" target="_blank" rel="noopener">GitHub</a>\n      </nav>|s' "$DOCS/$f"
  perl -i -0pe 's|<ul class="nav-list">\s*|<ul class="nav-list">\n            <li class="nav-item depth-0">\n              <a href="index.html" class="nav-link">Overview</a>\n            </li>\n            |s' "$DOCS/$f"
done

echo "Generated HTML in $DOCS (index.html is hand-maintained)."
