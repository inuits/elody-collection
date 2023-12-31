#!/bin/sh

export PATH=${PATH}:/app/.local/bin
export DB_ENGINE=$1
export REQUIRE_TOKEN=0
export STORAGE_API_URL=https://dams-storage-api.inuits.io
export STORAGE_API_URL_EXT=https://dams-storage-api.inuits.io
export IMAGE_API_URL_EXT=https://dams-image-api.inuits.io
export FLASK_ENV=development

cat << EOF
============================================
== Begin DAMS Collection API test results ==
============================================
EOF

coverage run -m pytest -s

cat << EOF
==========================================
== End DAMS Collection API test results ==
==========================================
EOF
