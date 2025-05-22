#pragma once

#include "binder/expression/rel_expression.h"
#include "common/enums/extend_direction.h"
#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/table/rel_table.h"

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

    ScanRelTableInfo(storage::RelTable* table, common::RelDataDirection direction,
        std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, direction{direction}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanRelTableInfo);

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
          direction{direction}, alias{std::move(alias)} {}

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

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<ScanRelTable>(info.copy(), relInfo.copy(), children[0]->copy(), id,
            printInfo->copy());
    }

protected:
    ScanRelTableInfo relInfo;
    std::unique_ptr<storage::RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
