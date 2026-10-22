#!/usr/bin/env bash
# Rewrite a Maven groupId in POM files. Does not commit.
set -euo pipefail

FROM_GROUP="${1:?from groupId required}"
TO_GROUP="${2:?to groupId required}"

if [ "${FROM_GROUP}" = "${TO_GROUP}" ]; then
  echo "GroupId already ${TO_GROUP}; nothing to rewrite"
  exit 0
fi

echo "Rewriting groupId ${FROM_GROUP} -> ${TO_GROUP}"
find . -name pom.xml \
  -not -path './.git/*' \
  -not -path '*/target/*' \
  -print0 |
while IFS= read -r -d '' pom; do
  sed -i "s|<groupId>${FROM_GROUP}</groupId>|<groupId>${TO_GROUP}</groupId>|g" "${pom}"
done
echo "Root groupId line: $(grep -m1 '<groupId>' pom.xml || true)"
