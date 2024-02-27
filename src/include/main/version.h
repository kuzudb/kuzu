#pragma once

#include "common/api.h"
#include "storage/storage_info.h"
namespace kuzu {
namespace main {

struct Version {
public:
    /**
     * @brief Get the version of the Kùzu library.
     * @return const char* The version of the Kùzu library.
     */
    KUZU_API inline static const char* getVersion() { return KUZU_CMAKE_VERSION; }

    /**
     * @brief Get the storage version of the Kùzu library.
     * @return uint64_t The storage version of the Kùzu library.
     */
    KUZU_API inline static uint64_t getStorageVersion() {
        return storage::StorageVersionInfo::getStorageVersion();
    }
};
} // namespace main
} // namespace kuzu