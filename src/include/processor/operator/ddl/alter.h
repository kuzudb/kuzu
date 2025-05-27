#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
class RelGroupCatalogEntry;
} // namespace catalog

namespace processor {

struct AlterPrintInfo final : OPPrintInfo {
    common::AlterType alterType;
    std::string tableName;
    binder::BoundAlterInfo info;

    AlterPrintInfo(common::AlterType alterType, std::string tableName, binder::BoundAlterInfo info)
        : alterType{alterType}, tableName{std::move(tableName)}, info{std::move(info)} {}

    std::string toString() const override { return info.toString(); }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<AlterPrintInfo>(new AlterPrintInfo(*this));
    }

private:
    AlterPrintInfo(const AlterPrintInfo& other)
        : OPPrintInfo{other}, alterType{other.alterType}, tableName{other.tableName},
          info{other.info.copy()} {}
};

class Alter final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::ALTER;

public:
    Alter(binder::BoundAlterInfo info,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        const DataPos& outputPos, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)},
          defaultValueEvaluator{std::move(defaultValueEvaluator)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<Alter>(info.copy(),
            defaultValueEvaluator == nullptr ? nullptr : defaultValueEvaluator->copy(), outputPos,
            id, printInfo->copy());
    }

private:
    void alterTable(main::ClientContext* clientContext, catalog::TableCatalogEntry* entry,
        const binder::BoundAlterInfo& alterInfo) const;

    binder::BoundAlterInfo info;
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
};

} // namespace processor
} // namespace kuzu
