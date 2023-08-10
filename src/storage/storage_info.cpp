#include "storage/storage_info.h"

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

storage_version_t StorageVersionInfo::getStorageVersion() {
    auto storageVersionInfo = getStorageVersionInfo();
    if (!storageVersionInfo.contains(KUZU_STORAGE_VERSION)) {
        throw RuntimeException(
            StringUtils::string_format("Invalid storage version name: {}", KUZU_STORAGE_VERSION));
    }
    return storageVersionInfo.at(KUZU_STORAGE_VERSION);
}

} // namespace storage
} // namespace kuzu
