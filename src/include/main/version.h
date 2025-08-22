#pragma once
#include <cstdint>
#include <unordered_map>
#include <string>
#include "common/api.h"
namespace kuzu {
namespace main {

struct Version {
public:
    /**
     * @brief Get the version of the Kuzu library.
     * @return const char* The version of the Kuzu library.
     */
    KUZU_API static const char* getVersion();

    /**
     * @brief Get the storage version of the Kuzu library.
     * @return uint64_t The storage version of the Kuzu library.
     */
    KUZU_API static uint64_t getStorageVersion();

    /**
     * @brief Get the storage version information of the Kuzu library.
     * @return std::unordered_map<std::string, uint64_t> The mapping from library version to the corresponding storage version.
     */
    KUZU_API static std::unordered_map<std::string, uint64_t> getStorageVersionInfo();
};
} // namespace main
} // namespace kuzu
