#pragma once

#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"
#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyFrom : public LogicalOperator {

public:
    LogicalCopyFrom(const common::CopyDescription& copyDescription, common::table_id_t tableID,
        std::string tableName, bool hasSerial, binder::expression_vector dataColumnExpressions,
        std::shared_ptr<binder::Expression> nodeOffsetExpression,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{LogicalOperatorType::COPY_FROM},
          copyDescription{copyDescription}, tableID{tableID}, tableName{std::move(tableName)},
          preservingOrder{hasSerial}, dataColumnExpressions{std::move(dataColumnExpressions)},
          nodeOffsetExpression{std::move(nodeOffsetExpression)}, outputExpression{
                                                                     std::move(outputExpression)} {}

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::vector<std::shared_ptr<binder::Expression>> getDataColumnExpressions() const {
        return dataColumnExpressions;
    }

    inline std::shared_ptr<binder::Expression> getNodeOffsetExpression() const {
        return nodeOffsetExpression;
    }

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline bool isPreservingOrder() { return preservingOrder; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyFrom>(copyDescription, tableID, tableName, preservingOrder,
            dataColumnExpressions, nodeOffsetExpression, outputExpression);
    }

private:
    common::CopyDescription copyDescription;
    common::table_id_t tableID;
    // Used for printing only.
    std::string tableName;
    binder::expression_vector dataColumnExpressions;
    std::shared_ptr<binder::Expression> nodeOffsetExpression;
    std::shared_ptr<binder::Expression> outputExpression;
    bool preservingOrder;
};

} // namespace planner
} // namespace kuzu
