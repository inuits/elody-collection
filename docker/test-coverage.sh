#!/bin/sh

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
