#pragma once

#include <mutex>

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

struct BaseScanSharedStateWithNumRows : public BaseScanSharedState {
    uint64_t numRows;

    explicit BaseScanSharedStateWithNumRows(uint64_t numRows) : numRows{numRows} {}

    uint64_t getNumRows() const override { return numRows; }
};

struct ScanSharedState : public BaseScanSharedStateWithNumRows {
    const common::ReaderConfig readerConfig;
    uint64_t fileIdx;
    uint64_t blockIdx;

    ScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows)
        : BaseScanSharedStateWithNumRows{numRows}, readerConfig{std::move(readerConfig)},
          fileIdx{0}, blockIdx{0} {}

    std::pair<uint64_t, uint64_t> getNext();
};

struct ScanFileSharedState : public ScanSharedState {
    main::ClientContext* context;
    uint64_t totalSize; // TODO(Mattias): I think we should unify the design on how we calculate the
                        // progress bar for scanning. Can we simply rely on a numRowsScaned stored
                        // in the TableFuncSharedState to determine the progress.
    ScanFileSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        main::ClientContext* context)
        : ScanSharedState{std::move(readerConfig), numRows}, context{context}, totalSize{0} {}
};

} // namespace function
} // namespace kuzu
