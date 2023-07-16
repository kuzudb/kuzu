#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class ScanRelTableColumns : public ScanRelTable, public SelVectorOverWriter {
public:
    ScanRelTableColumns(std::unique_ptr<RelTableScanInfo> scanInfo,
        std::unique_ptr<ScanRelTalePosInfo> posInfo, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : ScanRelTable{std::move(posInfo), PhysicalOperatorType::SCAN_REL_TABLE_COLUMNS,
              std::move(child), id, paramsString},
          scanInfo{std::move(scanInfo)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanRelTableColumns>(
            scanInfo->copy(), posInfo->copy(), children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<RelTableScanInfo> scanInfo;
    std::unique_ptr<storage::RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
