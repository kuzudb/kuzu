#include "storage/storage_info.h"

#include "common/exception/runtime.h"
#include "common/string_format.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

storage_version_t StorageVersionInfo::getStorageVersion() {
    auto storageVersionInfo = getStorageVersionInfo();
    if (!storageVersionInfo.contains(KUZU_STORAGE_VERSION)) {
        // LCOV_EXCL_START
        throw RuntimeException(
            stringFormat("Invalid storage version name: {}", KUZU_STORAGE_VERSION));
        // LCOV_EXCL_STOP
    }
    return storageVersionInfo.at(KUZU_STORAGE_VERSION);
}

} // namespace storage
} // namespace kuzu
