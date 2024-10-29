#pragma once

#include "binder/expression/rel_expression.h"
#include "common/enums/extend_direction.h"
#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {
class MemoryManager;
}
namespace processor {

struct ScanRelTableInfo {
    storage::RelTable* table;
    common::RelDataDirection direction;
    std::vector<common::column_id_t> columnIDs;
    std::vector<storage::ColumnPredicateSet> columnPredicates;

    std::unique_ptr<storage::RelTableScanState> scanState;

    ScanRelTableInfo(storage::RelTable* table, common::RelDataDirection direction,
        std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, direction{direction}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanRelTableInfo);

    void initScanState(const ExecutionContext* context);

private:
    ScanRelTableInfo(const ScanRelTableInfo& other)
        : table{other.table}, direction{other.direction}, columnIDs{other.columnIDs},
          columnPredicates{copyVector(other.columnPredicates)} {}
};

struct ScanRelTablePrintInfo final : OPPrintInfo {
    std::vector<std::string> tableNames;
    binder::expression_vector properties;
    std::shared_ptr<binder::NodeExpression> boundNode;
    std::shared_ptr<binder::RelExpression> rel;
    std::shared_ptr<binder::NodeExpression> nbrNode;
    common::ExtendDirection direction;
    std::string alias;

    ScanRelTablePrintInfo(std::vector<std::string> tableNames, binder::expression_vector properties,
        std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::RelExpression> rel, std::shared_ptr<binder::NodeExpression> nbrNode,
        common::ExtendDirection direction, std::string alias)
        : tableNames{std::move(tableNames)}, properties{std::move(properties)},
          boundNode{std::move(boundNode)}, rel{std::move(rel)}, nbrNode{std::move(nbrNode)},
          direction{std::move(direction)}, alias{std::move(alias)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<ScanRelTablePrintInfo>(new ScanRelTablePrintInfo(*this));
    }

private:
    ScanRelTablePrintInfo(const ScanRelTablePrintInfo& other)
        : OPPrintInfo{other}, tableNames{other.tableNames}, properties{other.properties},
          boundNode{other.boundNode}, rel{other.rel}, nbrNode{other.nbrNode},
          direction{other.direction}, alias{other.alias} {}
};

class ScanRelTable final : public ScanTable {
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
    void initVectors(storage::TableScanState& state, const ResultSet& resultSet) const override;

protected:
    ScanRelTableInfo relInfo;
};

} // namespace processor
} // namespace kuzu
