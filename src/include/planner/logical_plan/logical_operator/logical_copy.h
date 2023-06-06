#pragma once

#include "base_logical_operator.h"
#include "catalog/catalog_structs.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace planner {

class LogicalCopy : public LogicalOperator {

public:
    LogicalCopy(const common::CopyDescription& copyDescription, common::table_id_t tableID,
        std::string tableName, binder::expression_vector arrowColumnExpressions,
        std::shared_ptr<binder::Expression> offsetExpression,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{LogicalOperatorType::COPY}, copyDescription{copyDescription},
          tableID{tableID}, tableName{std::move(tableName)}, arrowColumnExpressions{std::move(
                                                                 arrowColumnExpressions)},
          offsetExpression{std::move(offsetExpression)}, outputExpression{
                                                             std::move(outputExpression)} {}

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::vector<std::shared_ptr<binder::Expression>> getArrowColumnExpressions() const {
        return arrowColumnExpressions;
    }

    inline std::shared_ptr<binder::Expression> getOffsetExpression() const {
        return offsetExpression;
    }

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopy>(copyDescription, tableID, tableName, arrowColumnExpressions,
            offsetExpression, outputExpression);
    }

private:
    common::CopyDescription copyDescription;
    common::table_id_t tableID;
    // Used for printing only.
    std::string tableName;
    binder::expression_vector arrowColumnExpressions;
    std::shared_ptr<binder::Expression> offsetExpression;
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
