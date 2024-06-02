#pragma once

#include <unordered_map>

#include "expression.h"

namespace kuzu {
namespace binder {

class NodeOrRelExpression : public Expression {
public:
    NodeOrRelExpression(common::LogicalType dataType, std::string uniqueName,
        std::string variableName, std::vector<common::table_id_t> tableIDs)
        : Expression{common::ExpressionType::PATTERN, std::move(dataType), std::move(uniqueName)},
          variableName(std::move(variableName)), tableIDs{std::move(tableIDs)} {}

    // Note: ideally I would try to remove this function. But for now, we have to create type
    // after expression.
    void setExtraTypeInfo(std::unique_ptr<common::ExtraTypeInfo> info) {
        dataType.setExtraTypeInfo(std::move(info));
    }

    std::string getVariableName() const { return variableName; }

    void setTableIDs(common::table_id_vector_t tableIDs_) { tableIDs = std::move(tableIDs_); }
    void addTableIDs(const common::table_id_vector_t& tableIDsToAdd);

    bool isEmpty() const { return tableIDs.empty(); }
    bool isMultiLabeled() const { return tableIDs.size() > 1; }
    uint32_t getNumTableIDs() const { return tableIDs.size(); }
    std::vector<common::table_id_t> getTableIDs() const { return tableIDs; }
    std::unordered_set<common::table_id_t> getTableIDsSet() const {
        return {tableIDs.begin(), tableIDs.end()};
    }
    common::table_id_t getSingleTableID() const;

    void addPropertyExpression(const std::string& propertyName,
        std::unique_ptr<Expression> property);
    bool hasPropertyExpression(const std::string& propertyName) const {
        return propertyNameToIdx.contains(propertyName);
    }
    // Deep copy expression.
    std::shared_ptr<Expression> getPropertyExpression(const std::string& propertyName) const;
    const std::vector<std::unique_ptr<Expression>>& getPropertyExprsRef() const {
        return propertyExprs;
    }
    // Deep copy expressions.
    expression_vector getPropertyExprs() const;

    bool hasPrimaryKey() const;
    std::shared_ptr<Expression> getPrimaryKey() const;

    void setLabelExpression(std::shared_ptr<Expression> expression) {
        labelExpression = std::move(expression);
    }
    std::shared_ptr<Expression> getLabelExpression() const { return labelExpression; }

    void addPropertyDataExpr(std::string propertyName, std::shared_ptr<Expression> expr) {
        propertyDataExprs.insert({propertyName, expr});
    }
    const std::unordered_map<std::string, std::shared_ptr<Expression>>&
    getPropertyDataExprRef() const {
        return propertyDataExprs;
    }
    bool hasPropertyDataExpr(const std::string& propertyName) const {
        return propertyDataExprs.contains(propertyName);
    }
    std::shared_ptr<Expression> getPropertyDataExpr(const std::string& propertyName) const {
        KU_ASSERT(propertyDataExprs.contains(propertyName));
        return propertyDataExprs.at(propertyName);
    }

    std::string toStringInternal() const final { return variableName; }

protected:
    std::string variableName;
    // A pattern may bind to multiple tables.
    std::vector<common::table_id_t> tableIDs;
    // Index over propertyExprs on property name.
    std::unordered_map<std::string, common::idx_t> propertyNameToIdx;
    // Property expressions with order (aligned with catalog).
    std::vector<std::unique_ptr<Expression>> propertyExprs;
    std::shared_ptr<Expression> labelExpression;
    // Property data expressions specified by user in the form of "{propertyName : data}"
    std::unordered_map<std::string, std::shared_ptr<Expression>> propertyDataExprs;
};

} // namespace binder
} // namespace kuzu
