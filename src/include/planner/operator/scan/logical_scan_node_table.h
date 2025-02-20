#pragma once

#include "binder/expression/expression_util.h"
#include "planner/operator/logical_operator.h"
#include "storage/predicate/column_predicate.h"

namespace kuzu {
namespace planner {

enum class LogicalScanNodeTableType : uint8_t {
    SCAN = 0,
    OFFSET_SCAN = 1,
    PRIMARY_KEY_SCAN = 2,
    MULTIPLE_OFFSET_SCAN = 3,
};

struct ExtraScanNodeTableInfo {
    virtual ~ExtraScanNodeTableInfo() = default;
    virtual std::unique_ptr<ExtraScanNodeTableInfo> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ExtraScanNodeTableInfo&, const TARGET&>(*this);
    }
};

// LogicalScanNodeTable now is also the source for recursive plan. Recursive plan node predicate
// need additional variable to evaluate. I cannot think of other operator that can put it into
// recursive plan schema.
struct RecursiveJoinScanInfo final : ExtraScanNodeTableInfo {
    std::shared_ptr<binder::Expression> nodePredicateExecFlag;

    explicit RecursiveJoinScanInfo(std::shared_ptr<binder::Expression> expr)
        : nodePredicateExecFlag{std::move(expr)} {}

    std::unique_ptr<ExtraScanNodeTableInfo> copy() const override {
        return std::make_unique<RecursiveJoinScanInfo>(nodePredicateExecFlag);
    }
};

struct PrimaryKeyScanInfo final : ExtraScanNodeTableInfo {
    std::shared_ptr<binder::Expression> key;

    explicit PrimaryKeyScanInfo(std::shared_ptr<binder::Expression> key) : key{std::move(key)} {}

    std::unique_ptr<ExtraScanNodeTableInfo> copy() const override {
        return std::make_unique<PrimaryKeyScanInfo>(key);
    }
};

class LogicalScanNodeTable final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::SCAN_NODE_TABLE;
    static constexpr LogicalScanNodeTableType defaultScanType = LogicalScanNodeTableType::SCAN;

public:
    LogicalScanNodeTable(std::shared_ptr<binder::Expression> nodeID,
                         std::vector<common::table_id_t> nodeTableIDs, binder::expression_vector properties,
                         common::node_group_idx_t startNodeGroupId = 0,
                         common::node_group_idx_t endNodeGroupId = common::INVALID_NODE_GROUP_IDX,
                         bool randomMorsels = false)
            : LogicalOperator{type_}, scanType{defaultScanType}, nodeID{std::move(nodeID)},
              nodeTableIDs{std::move(nodeTableIDs)}, properties{std::move(properties)},
              startNodeGroupId(startNodeGroupId), endNodeGroupId(endNodeGroupId), randomMorsels(randomMorsels) {}
    LogicalScanNodeTable(const LogicalScanNodeTable& other);

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override {
        std::string result = nodeID->toString() + " " + binder::ExpressionUtil::toString(properties);
        if (startNodeGroupId != 0 || endNodeGroupId != common::INVALID_NODE_GROUP_IDX) {
            result += " startNodeGroupId: " + std::to_string(startNodeGroupId) + " endNodeGroupId: " + std::to_string(endNodeGroupId);
        }
        return result;
    }

    LogicalScanNodeTableType getScanType() const { return scanType; }
    void setScanType(LogicalScanNodeTableType scanType_) { scanType = scanType_; }

    std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    std::vector<common::table_id_t> getTableIDs() const { return nodeTableIDs; }

    binder::expression_vector getProperties() const { return properties; }
    void setPropertyPredicates(std::vector<storage::ColumnPredicateSet> predicates) {
        propertyPredicates = std::move(predicates);
    }
    const std::vector<storage::ColumnPredicateSet>& getPropertyPredicates() const {
        return propertyPredicates;
    }

    void setExtraInfo(std::unique_ptr<ExtraScanNodeTableInfo> info) { extraInfo = std::move(info); }

    ExtraScanNodeTableInfo* getExtraInfo() const { return extraInfo.get(); }

    std::unique_ptr<ExtraScanNodeTableInfo> getExtraInfoCopy() const {
        return extraInfo->copy();
    }

    common::node_group_idx_t getStartNodeGroupId() const { return startNodeGroupId; }

    common::node_group_idx_t getEndNodeGroupId() const { return endNodeGroupId; }

    bool isRandomMorsels() const { return randomMorsels; }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    LogicalScanNodeTableType scanType;
    std::shared_ptr<binder::Expression> nodeID;
    std::vector<common::table_id_t> nodeTableIDs;
    binder::expression_vector properties;
    std::vector<storage::ColumnPredicateSet> propertyPredicates;
    std::unique_ptr<ExtraScanNodeTableInfo> extraInfo;
    common::node_group_idx_t startNodeGroupId;
    common::node_group_idx_t endNodeGroupId;
    bool randomMorsels = false;
};

} // namespace planner
} // namespace kuzu
