#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCommentOn : public LogicalOperator {
public:
    LogicalCommentOn(std::shared_ptr<binder::Expression> outputExpression,
        common::table_id_t tableID, std::string tableName, std::string comment)
        : LogicalOperator{LogicalOperatorType::COMMENT_ON}, outputExpression(outputExpression),
          tableID(tableID), tableName(tableName), comment(comment) {}

    inline common::table_id_t getTableID() const { return tableID; }
    inline std::string getTableName() const { return tableName; }
    inline std::string getComment() const { return comment; }

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCommentOn>(outputExpression, tableID, tableName, comment);
    }

private:
    std::shared_ptr<binder::Expression> outputExpression;
    common::table_id_t tableID;
    std::string tableName, comment;
};

} // namespace planner
} // namespace kuzu
