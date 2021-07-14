#!/bin/sh

export DB_ENGINE=memory
export REQUIRE_TOKEN=0
export STORAGE_API_URL=https://dams-storage-api.inuits.io

cat << EOF
============================================
== Begin DAMS Collection API test results ==
============================================
EOF

coverage run -m pytest

cat << EOF
==========================================
== End DAMS Collection API test results ==
==========================================
EOF
