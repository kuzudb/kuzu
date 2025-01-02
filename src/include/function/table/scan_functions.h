#pragma once

#include <mutex>

#include "common/copier_config/file_scan_info.h"
#include "function/table_functions.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct KUZU_API BaseScanSharedState : public TableFuncSharedState {
    std::mutex lock;

    virtual uint64_t getNumRows() const = 0;
};

struct BaseScanSharedStateWithNumRows : public BaseScanSharedState {
    uint64_t numRows;

    explicit BaseScanSharedStateWithNumRows(uint64_t numRows) : numRows{numRows} {}

    uint64_t getNumRows() const override { return numRows; }
};

struct ScanSharedState : public BaseScanSharedStateWithNumRows {
    const common::FileScanInfo fileScanInfo;
    uint64_t fileIdx;
    uint64_t blockIdx;

    ScanSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows)
        : BaseScanSharedStateWithNumRows{numRows}, fileScanInfo{std::move(fileScanInfo)},
          fileIdx{0}, blockIdx{0} {}

    std::pair<uint64_t, uint64_t> getNext();
};

struct ScanFileSharedState : public ScanSharedState {
    main::ClientContext* context;
    uint64_t totalSize; // TODO(Mattias): I think we should unify the design on how we calculate the
                        // progress bar for scanning. Can we simply rely on a numRowsScaned stored
                        // in the TableFuncSharedState to determine the progress.
    ScanFileSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows,
        main::ClientContext* context)
        : ScanSharedState{std::move(fileScanInfo), numRows}, context{context}, totalSize{0} {}
};

} // namespace function
} // namespace kuzu
