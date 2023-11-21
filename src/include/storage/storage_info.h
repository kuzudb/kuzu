#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace kuzu {
namespace storage {

using storage_version_t = uint64_t;

struct StorageVersionInfo {
    static std::unordered_map<std::string, storage_version_t> getStorageVersionInfo() {
        return {{"0.1.0", 24}, {"0.0.12.3", 24}, {"0.0.12.2", 24}, {"0.0.12.1", 24}, {"0.0.12", 23},
            {"0.0.11", 23}, {"0.0.10", 23}, {"0.0.9", 23}, {"0.0.8", 17}, {"0.0.7", 15},
            {"0.0.6", 9}, {"0.0.5", 8}, {"0.0.4", 7}, {"0.0.3", 1}};
    }

    static storage_version_t getStorageVersion();

    static constexpr const char* MAGIC_BYTES = "KUZU";
};

} // namespace storage
} // namespace kuzu
