#include "main/version.h"

#include "storage/storage_version_info.h"

namespace kuzu {
namespace main {
const char* Version::getVersion() {
    return KUZU_CMAKE_VERSION;
}

uint64_t Version::getStorageVersion() {
    return storage::StorageVersionInfo::getStorageVersion();
}
} // namespace main
} // namespace kuzu
