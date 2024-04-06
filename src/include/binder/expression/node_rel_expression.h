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
    ~NodeOrRelExpression() override = default;

    inline std::string getVariableName() const { return variableName; }

    inline void setTableIDs(common::table_id_vector_t tableIDs) {
        this->tableIDs = std::move(tableIDs);
    }
    inline void addTableIDs(const common::table_id_vector_t& tableIDsToAdd) {
        auto tableIDsSet = getTableIDsSet();
        for (auto tableID : tableIDsToAdd) {
            if (!tableIDsSet.contains(tableID)) {
                tableIDs.push_back(tableID);
            }
        }
    }

    inline bool isMultiLabeled() const { return tableIDs.size() > 1; }
    inline uint32_t getNumTableIDs() const { return tableIDs.size(); }
    inline std::vector<common::table_id_t> getTableIDs() const { return tableIDs; }
    inline std::unordered_set<common::table_id_t> getTableIDsSet() const {
        return {tableIDs.begin(), tableIDs.end()};
    }
    inline common::table_id_t getSingleTableID() const {
        KU_ASSERT(tableIDs.size() == 1);
        return tableIDs[0];
    }

    inline void addPropertyExpression(const std::string& propertyName,
        std::unique_ptr<Expression> property) {
        KU_ASSERT(!propertyNameToIdx.contains(propertyName));
        propertyNameToIdx.insert({propertyName, propertyExprs.size()});
        propertyExprs.push_back(std::move(property));
    }
    inline bool hasPropertyExpression(const std::string& propertyName) const {
        return propertyNameToIdx.contains(propertyName);
    }
    inline std::shared_ptr<Expression> getPropertyExpression(
        const std::string& propertyName) const {
        KU_ASSERT(propertyNameToIdx.contains(propertyName));
        return propertyExprs[propertyNameToIdx.at(propertyName)]->copy();
    }
    inline const std::vector<std::unique_ptr<Expression>>& getPropertyExprsRef() const {
        return propertyExprs;
    }
    inline expression_vector getPropertyExprs() const {
        expression_vector result;
        for (auto& expr : propertyExprs) {
            result.push_back(expr->copy());
        }
        return result;
    }

    inline void setLabelExpression(std::shared_ptr<Expression> expression) {
        labelExpression = std::move(expression);
    }
    inline std::shared_ptr<Expression> getLabelExpression() const { return labelExpression; }

    inline void addPropertyDataExpr(std::string propertyName, std::shared_ptr<Expression> expr) {
        propertyDataExprs.insert({propertyName, expr});
    }
    inline const std::unordered_map<std::string, std::shared_ptr<Expression>>&
    getPropertyDataExprRef() const {
        return propertyDataExprs;
    }
    inline bool hasPropertyDataExpr(const std::string& propertyName) const {
        return propertyDataExprs.contains(propertyName);
    }
    inline std::shared_ptr<Expression> getPropertyDataExpr(const std::string& propertyName) const {
        KU_ASSERT(propertyDataExprs.contains(propertyName));
        return propertyDataExprs.at(propertyName);
    }

    inline std::string toStringInternal() const final { return variableName; }

protected:
    std::string variableName;
    // A pattern may bind to multiple tables.
    std::vector<common::table_id_t> tableIDs;
    // Index over propertyExprs on property name.
    std::unordered_map<std::string, common::vector_idx_t> propertyNameToIdx;
    // Property expressions with order (aligned with catalog).
    std::vector<std::unique_ptr<Expression>> propertyExprs;
    std::shared_ptr<Expression> labelExpression;
    // Property data expressions specified by user in the form of "{propertyName : data}"
    std::unordered_map<std::string, std::shared_ptr<Expression>> propertyDataExprs;
};

} // namespace binder
} // namespace kuzu
