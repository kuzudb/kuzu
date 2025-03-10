#pragma once

#include "processor/operator/physical_operator.h"
#include "scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/table.h"

namespace kuzu {
namespace processor {

struct OffsetScanTableInfo {
    storage::Table* table;
    std::vector<common::column_id_t> columnIDs;
    std::vector<storage::ColumnPredicateSet> columnPredicates;

    OffsetScanTableInfo(storage::Table* table, std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(OffsetScanTableInfo);

private:
    OffsetScanTableInfo(const OffsetScanTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs},
          columnPredicates{copyVector(other.columnPredicates)} {}
};

class OffsetScanTable final : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::OFFSET_SCAN_TABLE;

public:
    OffsetScanTable(ScanTableInfo info, OffsetScanTableInfo offsetScanInfo, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), id, std::move(printInfo)}, IDVector{nullptr},
          offsetScanInfo{std::move(offsetScanInfo)} {}

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<OffsetScanTable>(info.copy(), offsetScanInfo.copy(), id,
            printInfo->copy());
    }

private:
    common::ValueVector* IDVector;
    OffsetScanTableInfo offsetScanInfo;
};

} // namespace processor
} // namespace kuzu
