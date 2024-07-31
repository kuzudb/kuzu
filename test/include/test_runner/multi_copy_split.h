#pragma once

#include <string>
#include <vector>

#include "main/kuzu.h"

namespace kuzu {
namespace testing {

// Insert a dataset row by row instead of batch insert (copy)
class SplitMultiCopyRandom {
public:
    SplitMultiCopyRandom(uint32_t numSplit, std::string tableName, std::string source, main::Connection& connection)
        : numSplit{numSplit}, tableName{tableName}, source{source}, connection{connection} {}
    void init();
    void run();

private:
    std::vector<uint32_t> getLineEnds(std::string path) const;

private:
    uint32_t numSplit;
    std::string tableName;
    std::string source;
    main::Connection& connection;
    std::vector<std::string> splitFilePaths;
};

} // namespace testing
} // namespace kuzu
