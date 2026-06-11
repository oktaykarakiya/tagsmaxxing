#!/usr/bin/env bash
# add-license-headers.sh — add SPDX header to every Rust source file under crates/
#
# Prepend "// SPDX-License-Identifier: AGPL-3.0-or-later" before the first
# non-shebang, non-comment line. Skips files that already carry an SPDX tag.
# Run from the workspace root.
set -euo pipefail

SPDX_TAG="AGPL-3.0-or-later"
HEADER="// SPDX-License-Identifier: ${SPDX_TAG}"

added=0
skipped=0
failures=0

while IFS= read -r file; do
    # Skip target/ directories
    case "$file" in
        */target/*) continue ;;
    esac

    # Skip if already has an SPDX tag
    if grep -q 'SPDX-License-Identifier:' "$file" 2>/dev/null; then
        skipped=$((skipped + 1))
        continue
    fi

    # Read the first line
    first_line=$(head -n1 "$file")

    # Determine insertion point:
    # - If file starts with #! (shebang), insert after it
    # - Otherwise, prepend before everything
    if [[ "$first_line" =~ ^#! ]]; then
        # Insert after the shebang line
        tmp=$(mktemp)
        {
            head -n1 "$file"
            # Add a blank line separator if the second line isn't already blank
            if sed -n '2p' "$file" | grep -q '[^[:space:]]'; then
                echo ""
            fi
            echo "$HEADER"
            echo ""
            tail -n +2 "$file"
        } > "$tmp"
    else
        # Prepend the header with a trailing blank line
        tmp=$(mktemp)
        {
            echo "$HEADER"
            echo ""
            cat "$file"
        } > "$tmp"
    fi

    mv "$tmp" "$file"
    added=$((added + 1))
done < <(find crates -name '*.rs' ! -path '*/target/*')

echo "SPDX header insertion complete."
echo "  Added:   ${added}"
echo "  Skipped: ${skipped} (already had SPDX)"
echo "  Failures: ${failures}"
