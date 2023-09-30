#!/bin/bash

cd /home/runner/actions-runner

# Get registration token
REG_TOKEN=$(curl \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_ACCESS_TOKEN}" \
    https://api.github.com/repos/kuzudb/kuzu/actions/runners/registration-token | jq .token --raw-output)

LABELS="kuzu-self-hosted-linux-building-aarch64"
if [ -z "${MACHINE_NAME}" ]; then
    echo "MACHINE_NAME is not set. The label is ignored."
else
    LABELS="kuzu-self-hosted-linux-building-aarch64,$MACHINE_NAME"
fi

# Register runner
./config.sh --url https://github.com/kuzudb/kuzu --token $REG_TOKEN --name --unattended --labels $LABELS

cleanup() {
    echo "Removing runner..."
    REMOVE_TOKEN=$(curl \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_ACCESS_TOKEN}" \
        https://api.github.com/repos/kuzudb/kuzu/actions/runners/remove-token | jq .token --raw-output)

    ./config.sh remove --token ${REMOVE_TOKEN}
}

trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

./run.sh &
wait $!
