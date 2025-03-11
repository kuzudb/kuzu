#pragma once

#include <mutex>

#include "common/copier_config/file_scan_info.h"
#include "function/table/table_function.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct ScanFileSharedState : public TableFuncSharedState {
    const common::FileScanInfo fileScanInfo;
    uint64_t fileIdx;
    uint64_t blockIdx;

    ScanFileSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows)
        : TableFuncSharedState{numRows}, fileScanInfo{std::move(fileScanInfo)}, fileIdx{0},
          blockIdx{0} {}

    std::pair<uint64_t, uint64_t> getNext() {
        std::lock_guard guard{mtx};
        return fileIdx >= fileScanInfo.getNumFiles() ? std::make_pair(UINT64_MAX, UINT64_MAX) :
                                                       std::make_pair(fileIdx, blockIdx++);
    }
};

struct ScanFileWithProgressSharedState : ScanFileSharedState {
    main::ClientContext* context;
    uint64_t totalSize; // TODO(Mattias): I think we should unify the design on how we calculate the
                        // progress bar for scanning. Can we simply rely on a numRowsScaned stored
                        // in the TableFuncSharedState to determine the progress.
    ScanFileWithProgressSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows,
        main::ClientContext* context)
        : ScanFileSharedState{std::move(fileScanInfo), numRows}, context{context}, totalSize{0} {}
};

} // namespace function
} // namespace kuzu
