tar --exclude="$(pwd)" \
    --exclude="./bazel-*" \
    --exclude="./scripts" \
    --exclude="./.?*" \
    -cf\
    graphflowdb.tar\
    -C ../../. .

tar -rf graphflowdb.tar ./setup.py ./MANIFEST.in ./graphflowdb
gzip graphflowdb.tar
