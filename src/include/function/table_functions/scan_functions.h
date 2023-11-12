#pragma once

#include "function/table_functions.h"

namespace kuzu {
namespace function {

struct BaseScanSharedState : public TableFuncSharedState {
    std::mutex lock;
    uint64_t fileIdx;
    uint64_t blockIdx;
    uint64_t numRows;

    BaseScanSharedState(uint64_t numRows) : fileIdx{0}, blockIdx{0}, numRows{numRows} {}
};

struct ScanSharedState : public BaseScanSharedState {
    const common::ReaderConfig readerConfig;

    ScanSharedState(const common::ReaderConfig readerConfig, uint64_t numRows)
        : BaseScanSharedState{numRows}, readerConfig{std::move(readerConfig)} {}

    std::pair<uint64_t, uint64_t> getNext();
};

} // namespace function
} // namespace kuzu
