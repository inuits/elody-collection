#!/bin/sh

export PATH=${PATH}:/app/.local/bin

cat << EOF
=============================================
== Begin DAMS Collection API test coverage ==
=============================================
EOF

coverage report -m

cat << EOF
===========================================
== End DAMS Collection API test coverage ==
===========================================
EOF
