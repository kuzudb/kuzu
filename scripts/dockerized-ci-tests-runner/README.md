## Build
```
docker build -t kuzu-self-hosted-test-runner .
```

## Start Container
```
docker run  --name self-hosted-test-runner-X --detach --restart=always\
            -e GITHUB_ACCESS_TOKEN=YOUR_GITHUB_ACCESS_TOKEN\
            -e MACHINE_NAME=NAME_OF_THE_PHYSICAL_MACHINE kuzu-self-hosted-test-runner
```

Note: `GITHUB_ACCESS_TOKEN` is the account-level access token that can be acquired at [GitHub developer settings](https://github.com/settings/tokens).