#pragma once

#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalGDSCall final : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::GDS_CALL;

public:
    explicit LogicalGDSCall(binder::BoundGDSCallInfo info)
        : LogicalOperator{operatorType_}, info{std::move(info)} {}
    LogicalGDSCall(binder::BoundGDSCallInfo info, common::table_id_set_t nbrTableIDSet,
        std::shared_ptr<LogicalOperator> nodePredicateRoot)
        : LogicalOperator{operatorType_}, info{std::move(info)}, nbrTableIDSet{nbrTableIDSet},
          nodePredicateRoot{std::move(nodePredicateRoot)} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    const binder::BoundGDSCallInfo& getInfo() const { return info; }
    binder::BoundGDSCallInfo& getInfoUnsafe() { return info; }

    void setNbrTableIDSet(common::table_id_set_t set) { nbrTableIDSet = set; }
    bool hasNbrTableIDSet() const { return !nbrTableIDSet.empty(); }
    common::table_id_set_t getNbrTableIDSet() const { return nbrTableIDSet; }

    void setNodePredicateRoot(std::shared_ptr<LogicalOperator> op) {
        nodePredicateRoot = std::move(op);
    }
    bool hasNodePredicate() const { return nodePredicateRoot != nullptr; }
    std::shared_ptr<LogicalOperator> getNodePredicateRoot() const { return nodePredicateRoot; }

    std::string getExpressionsForPrinting() const override { return info.func.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalGDSCall>(info.copy(), nbrTableIDSet, nodePredicateRoot);
    }

private:
    binder::BoundGDSCallInfo info;
    common::table_id_set_t nbrTableIDSet;
    std::shared_ptr<LogicalOperator> nodePredicateRoot;
};

} // namespace planner
} // namespace kuzu
