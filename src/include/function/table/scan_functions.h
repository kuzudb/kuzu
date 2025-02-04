#pragma once

#include <mutex>

#include "common/copier_config/file_scan_info.h"
#include "function/table/table_function.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct KUZU_API BaseScanSharedState : TableFuncSharedState {
    std::mutex lock;

    virtual uint64_t getNumRows() const = 0;
};

struct BaseScanSharedStateWithNumRows : BaseScanSharedState {
    uint64_t numRows;

    explicit BaseScanSharedStateWithNumRows(uint64_t numRows) : numRows{numRows} {}

    uint64_t getNumRows() const override { return numRows; }
};

struct ScanSharedState : BaseScanSharedStateWithNumRows {
    const common::FileScanInfo fileScanInfo;
    uint64_t fileIdx;
    uint64_t blockIdx;

    ScanSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows)
        : BaseScanSharedStateWithNumRows{numRows}, fileScanInfo{std::move(fileScanInfo)},
          fileIdx{0}, blockIdx{0} {}

    std::pair<uint64_t, uint64_t> getNext() {
        std::lock_guard guard{lock};
        return fileIdx >= fileScanInfo.getNumFiles() ? std::make_pair(UINT64_MAX, UINT64_MAX) :
                                                       std::make_pair(fileIdx, blockIdx++);
    }
};

struct ScanFileSharedState : ScanSharedState {
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
