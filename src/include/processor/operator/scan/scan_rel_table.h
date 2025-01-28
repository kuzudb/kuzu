#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/rel_table.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace processor {

struct ScanRelTableInfo {
    storage::RelTable* table;
    common::RelDataDirection direction;
    std::vector<common::column_id_t> columnIDs;
    std::vector<storage::ColumnPredicateSet> columnPredicates;

    std::vector<std::unique_ptr<storage::RelTableScanState>> localScanStates;

    std::unique_ptr<storage::RelTableScanState> localScanState;

    ScanRelTableInfo(storage::RelTable* table, common::RelDataDirection direction,
        std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, direction{direction}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanRelTableInfo);

    void initScanState();

private:
    ScanRelTableInfo(const ScanRelTableInfo& other)
        : table{other.table}, direction{other.direction}, columnIDs{other.columnIDs},
          columnPredicates{copyVector(other.columnPredicates)} {}
};

class ScanRelTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_REL_TABLE;

public:
    ScanRelTable(ScanTableInfo info, ScanRelTableInfo relInfo,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), std::move(child), id, std::move(printInfo)},
          relInfo{std::move(relInfo)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanRelTable>(info.copy(), relInfo.copy(), children[0]->clone(), id,
            printInfo->copy());
    }

private:
    inline storage::RelTableScanState* getRelScanState() {
        auto selSize = nodeIDVector->state->getSelVector().getSelSize();
        if (selSize <= 0) {
            return nullptr;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getSelVector()[0]);
        auto nodeGroupIdx = storage::StorageUtils::getNodeGroupIdx(nodeOffset);
        return relInfo.localScanStates[nodeGroupIdx].get();
    }

protected:
    ScanRelTableInfo relInfo;
};

} // namespace processor
} // namespace kuzu
