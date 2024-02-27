#include "main/version.h"

#include "c_api/helpers.h"
#include "c_api/kuzu.h"

char* kuzu_get_version() {
    return convertToOwnedCString(kuzu::main::Version::getVersion());
}

uint64_t kuzu_get_storage_version() {
    return kuzu::main::Version::getStorageVersion();
}
