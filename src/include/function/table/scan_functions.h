#pragma once

#include "common/copier_config/reader_config.h"
#include "function/table_functions.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct BaseScanSharedState : public TableFuncSharedState {
    std::mutex lock;

    virtual uint64_t getNumRows() const = 0;
};

struct BaseFileScanSharedState : public BaseScanSharedState {
    uint64_t fileIdx;
    uint64_t blockIdx;
    uint64_t numRows;

    explicit BaseFileScanSharedState(uint64_t numRows)
        : BaseScanSharedState{}, fileIdx{0}, blockIdx{0}, numRows{numRows} {}

    uint64_t getNumRows() const override { return numRows; }
};

struct ScanSharedState : public BaseFileScanSharedState {
    const common::ReaderConfig readerConfig;

    ScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows)
        : BaseFileScanSharedState{numRows}, readerConfig{std::move(readerConfig)} {}

    std::pair<uint64_t, uint64_t> getNext();
};

struct ScanFileSharedState : public ScanSharedState {
    main::ClientContext* context;
    uint64_t totalSize;

    ScanFileSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        main::ClientContext* context)
        : ScanSharedState{std::move(readerConfig), numRows}, context{context}, totalSize{0} {}
};

} // namespace function
} // namespace kuzu
