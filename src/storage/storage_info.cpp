#include "storage/storage_info.h"

namespace kuzu {
namespace storage {

storage_version_t StorageVersionInfo::getStorageVersion() {
    auto storageVersionInfo = getStorageVersionInfo();
    if (!storageVersionInfo.contains(KUZU_CMAKE_VERSION)) {
        // If the current KUZU_CMAKE_VERSION is not in the map,
        // then we must run the newest version of kuzu
        // LCOV_EXCL_START
        storage_version_t maxVersion = 0;
        for (auto& [_, versionNumber] : storageVersionInfo) {
            maxVersion = std::max(maxVersion, versionNumber);
        }
        return maxVersion;
        // LCOV_EXCL_STOP
    }
    return storageVersionInfo.at(KUZU_CMAKE_VERSION);
}

} // namespace storage
} // namespace kuzu
