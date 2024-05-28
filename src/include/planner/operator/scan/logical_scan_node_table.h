#pragma once

#include "binder/expression/expression_util.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

enum class LogicalScanNodeTableType : uint8_t {
    SCAN = 0,
    OFFSET_LOOK_UP = 1,
};

// LogicalScanNodeTable now is also the source for recursive plan. Recursive plan node predicate
// need additional variable to evaluate. I cannot think of other operator that can put it into
// recursive plan schema.
struct RecursiveJoinScanInfo {
    std::shared_ptr<binder::Expression> nodePredicateExecFlag;

    explicit RecursiveJoinScanInfo(std::shared_ptr<binder::Expression> expr)
        : nodePredicateExecFlag{std::move(expr)} {}

    std::unique_ptr<RecursiveJoinScanInfo> copy() const {
        return std::make_unique<RecursiveJoinScanInfo>(nodePredicateExecFlag);
    }
};

class LogicalScanNodeTable final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::SCAN_NODE_TABLE;
    static constexpr LogicalScanNodeTableType defaultScanType = LogicalScanNodeTableType::SCAN;

public:
    LogicalScanNodeTable(std::shared_ptr<binder::Expression> nodeID,
        std::vector<common::table_id_t> nodeTableIDs, binder::expression_vector properties)
        : LogicalOperator{type_}, scanType{defaultScanType}, nodeID{std::move(nodeID)},
          nodeTableIDs{std::move(nodeTableIDs)}, properties{std::move(properties)} {}
    LogicalScanNodeTable(const LogicalScanNodeTable& other);

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(properties);
    }

    void setScanType(LogicalScanNodeTableType scanType_) { scanType = scanType_; }
    LogicalScanNodeTableType getScanType() const { return scanType; }

    std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    std::vector<common::table_id_t> getTableIDs() const { return nodeTableIDs; }
    binder::expression_vector getProperties() const { return properties; }

    void setRecursiveJoinScanInfo(std::unique_ptr<RecursiveJoinScanInfo> info) {
        recursiveJoinScanInfo = std::move(info);
    }
    bool hasRecursiveJoinScanInfo() const { return recursiveJoinScanInfo != nullptr; }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    LogicalScanNodeTableType scanType;
    std::shared_ptr<binder::Expression> nodeID;
    std::vector<common::table_id_t> nodeTableIDs;
    binder::expression_vector properties;
    std::unique_ptr<RecursiveJoinScanInfo> recursiveJoinScanInfo;
};

} // namespace planner
} // namespace kuzu
