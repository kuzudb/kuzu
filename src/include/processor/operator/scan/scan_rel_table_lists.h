#pragma once

#include "processor/operator/scan/scan_rel_table.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class ScanRelTableLists : public ScanRelTable {
public:
    ScanRelTableLists(std::unique_ptr<RelTableScanInfo> scanInfo,
        std::unique_ptr<ScanRelTalePosInfo> posInfo, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : ScanRelTable{std::move(posInfo), PhysicalOperatorType::SCAN_REL_TABLE_LISTS,
              std::move(child), id, paramsString},
          scanInfo{std::move(scanInfo)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelTableLists>(
            scanInfo->copy(), posInfo->copy(), children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<RelTableScanInfo> scanInfo;
    std::unique_ptr<storage::RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
