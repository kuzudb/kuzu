#pragma once

#include "common/case_insensitive_map.h"
#include "expression.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
}
namespace binder {

class NodeOrRelExpression : public Expression {
    static constexpr common::ExpressionType expressionType_ = common::ExpressionType::PATTERN;

public:
    NodeOrRelExpression(common::LogicalType dataType, std::string uniqueName,
        std::string variableName, std::vector<catalog::TableCatalogEntry*> entries)
        : Expression{expressionType_, std::move(dataType), std::move(uniqueName)},
          variableName(std::move(variableName)), entries{std::move(entries)} {}

    // Note: ideally I would try to remove this function. But for now, we have to create type
    // after expression.
    void setExtraTypeInfo(std::unique_ptr<common::ExtraTypeInfo> info) {
        dataType.setExtraTypeInfo(std::move(info));
    }

    std::string getVariableName() const { return variableName; }

    bool isEmpty() const { return entries.empty(); }
    bool isMultiLabeled() const { return entries.size() > 1; }
    common::idx_t getNumEntries() const { return entries.size(); }
    common::table_id_vector_t getTableIDs() const;
    common::table_id_set_t getTableIDsSet() const;
    const std::vector<catalog::TableCatalogEntry*>& getEntries() const { return entries; }
    void setEntries(std::vector<catalog::TableCatalogEntry*> entries_) {
        entries = std::move(entries_);
    }
    void addEntries(const std::vector<catalog::TableCatalogEntry*> entries_);
    catalog::TableCatalogEntry* getSingleEntry() const;

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

    void setLabelExpression(std::shared_ptr<Expression> expression) {
        labelExpression = std::move(expression);
    }
    std::shared_ptr<Expression> getLabelExpression() const { return labelExpression; }

    void addPropertyDataExpr(std::string propertyName, std::shared_ptr<Expression> expr) {
        propertyDataExprs.insert({propertyName, expr});
    }
    const common::case_insensitive_map_t<std::shared_ptr<Expression>>&
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
    std::vector<catalog::TableCatalogEntry*> entries;
    // Index over propertyExprs on property name.
    common::case_insensitive_map_t<common::idx_t> propertyNameToIdx;
    // Property expressions with order (aligned with catalog).
    std::vector<std::unique_ptr<Expression>> propertyExprs;
    std::shared_ptr<Expression> labelExpression;
    // Property data expressions specified by user in the form of "{propertyName : data}"
    common::case_insensitive_map_t<std::shared_ptr<Expression>> propertyDataExprs;
};

} // namespace binder
} // namespace kuzu
