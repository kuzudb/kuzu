#pragma once

#include "function/table_functions.h"

namespace kuzu {
namespace function {

struct ScanSharedTableFuncState : public TableFuncSharedState {
    std::mutex lock;
    uint64_t fileIdx;
    uint64_t blockIdx;
    const common::ReaderConfig readerConfig;
    uint64_t numRows;

    ScanSharedTableFuncState(const common::ReaderConfig readerConfig, uint64_t numRows)
        : fileIdx{0}, blockIdx{0}, readerConfig{std::move(readerConfig)}, numRows{numRows} {}

    std::pair<uint64_t, uint64_t> getNext();
};

} // namespace function
} // namespace kuzu
