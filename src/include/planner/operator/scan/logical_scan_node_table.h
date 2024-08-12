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

struct LogicalNodeTableScanInfo {
    common::table_id_t tableID;
    std::vector<common::column_id_t> columnIDs;

    LogicalNodeTableScanInfo(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs)
        : tableID{tableID}, columnIDs{std::move(columnIDs)} {}
};

class LogicalScanNodeTable final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::SCAN_NODE_TABLE;
    static constexpr LogicalScanNodeTableType defaultScanType = LogicalScanNodeTableType::SCAN;

public:
    LogicalScanNodeTable(std::shared_ptr<binder::Expression> nodeID,
        binder::expression_vector properties, std::vector<LogicalNodeTableScanInfo> tableScanInfos)
        : LogicalOperator{type_}, scanType{defaultScanType}, nodeID{std::move(nodeID)},
          properties{std::move(properties)}, tableScanInfos{std::move(tableScanInfos)} {}
    LogicalScanNodeTable(const LogicalScanNodeTable& other);

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override {
        return nodeID->toString() + " " + binder::ExpressionUtil::toString(properties);
    }

    LogicalScanNodeTableType getScanType() const { return scanType; }
    void setScanType(LogicalScanNodeTableType scanType_) { scanType = scanType_; }

    std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }

    const std::vector<LogicalNodeTableScanInfo>& getTableScanInfos() const {
        return tableScanInfos;
    }
    std::vector<common::table_id_t> getTableIDs() const;

    binder::expression_vector getProperties() const { return properties; }
    void setPropertyPredicates(std::vector<storage::ColumnPredicateSet> predicates) {
        propertyPredicates = std::move(predicates);
    }
    const std::vector<storage::ColumnPredicateSet>& getPropertyPredicates() const {
        return propertyPredicates;
    }

    void setExtraInfo(std::unique_ptr<ExtraScanNodeTableInfo> info) { extraInfo = std::move(info); }

    ExtraScanNodeTableInfo* getExtraInfo() const { return extraInfo.get(); }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    LogicalScanNodeTableType scanType;
    std::shared_ptr<binder::Expression> nodeID;
    binder::expression_vector properties;
    std::vector<LogicalNodeTableScanInfo> tableScanInfos;
    std::vector<storage::ColumnPredicateSet> propertyPredicates;
    std::unique_ptr<ExtraScanNodeTableInfo> extraInfo;
};

} // namespace planner
} // namespace kuzu
