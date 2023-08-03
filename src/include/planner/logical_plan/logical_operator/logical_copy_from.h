#pragma once

#include "base_logical_operator.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace planner {

class LogicalCopyFrom : public LogicalOperator {

public:
    LogicalCopyFrom(const common::CopyDescription& copyDescription, common::table_id_t tableID,
        std::string tableName, binder::expression_vector dataColumnExpressions,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{LogicalOperatorType::COPY_FROM},
          copyDescription{copyDescription}, tableID{tableID}, tableName{std::move(tableName)},
          dataColumnExpressions{std::move(dataColumnExpressions)}, outputExpression{std::move(
                                                                       outputExpression)} {}

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::vector<std::shared_ptr<binder::Expression>> getDataColumnExpressions() const {
        return dataColumnExpressions;
    }

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyFrom>(
            copyDescription, tableID, tableName, dataColumnExpressions, outputExpression);
    }

private:
    common::CopyDescription copyDescription;
    common::table_id_t tableID;
    // Used for printing only.
    std::string tableName;
    binder::expression_vector dataColumnExpressions;
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
