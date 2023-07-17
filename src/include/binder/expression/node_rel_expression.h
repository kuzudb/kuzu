#pragma once

#include <unordered_map>

#include "expression.h"

namespace kuzu {
namespace binder {

class NodeOrRelExpression : public Expression {
public:
    NodeOrRelExpression(common::LogicalType dataType, std::string uniqueName,
        std::string variableName, std::vector<common::table_id_t> tableIDs)
        : Expression{common::VARIABLE, std::move(dataType), std::move(uniqueName)},
          variableName(std::move(variableName)), tableIDs{std::move(tableIDs)} {}
    virtual ~NodeOrRelExpression() override = default;

    inline std::string getVariableName() const { return variableName; }

    inline void addTableIDs(const std::vector<common::table_id_t>& tableIDsToAdd) {
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
        assert(tableIDs.size() == 1);
        return tableIDs[0];
    }

    inline void addPropertyExpression(
        const std::string& propertyName, std::unique_ptr<Expression> property) {
        assert(!propertyNameToIdx.contains(propertyName));
        propertyNameToIdx.insert({propertyName, properties.size()});
        properties.push_back(std::move(property));
    }
    inline bool hasPropertyExpression(const std::string& propertyName) const {
        return propertyNameToIdx.contains(propertyName);
    }
    inline std::shared_ptr<Expression> getPropertyExpression(
        const std::string& propertyName) const {
        assert(propertyNameToIdx.contains(propertyName));
        return properties[propertyNameToIdx.at(propertyName)]->copy();
    }
    inline const std::vector<std::unique_ptr<Expression>>& getPropertyExpressions() const {
        return properties;
    }

    inline void setLabelExpression(std::shared_ptr<Expression> expression) {
        labelExpression = std::move(expression);
    }
    inline std::shared_ptr<Expression> getLabelExpression() const { return labelExpression; }

    std::string toString() const override { return variableName; }

protected:
    std::string variableName;
    std::vector<common::table_id_t> tableIDs;
    std::unordered_map<std::string, common::vector_idx_t> propertyNameToIdx;
    std::vector<std::unique_ptr<Expression>> properties;
    std::shared_ptr<Expression> labelExpression;
};

} // namespace binder
} // namespace kuzu
