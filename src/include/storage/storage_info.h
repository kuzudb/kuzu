#pragma once

#include "exception"
#include <string>

#include "common/utils.h"

namespace kuzu {
namespace storage {

using storage_version_t = uint64_t;

struct StorageVersionInfo {
    static std::unordered_map<std::string, storage_version_t> getStorageVersionInfo() {
        return {{"0.0.3", 1}};
    }

    static storage_version_t getStorageVersion();

    static constexpr const char* MAGIC_BYTES = "KUZU";
};

} // namespace storage
} // namespace kuzu
