#include "planner/operator/persistent/logical_insert.h"

#include "binder/expression/node_expression.h"
#include "common/cast.h"
#include "planner/operator/factorization/flatten_resolver.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void LogicalInsert::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        for (auto i = 0u; i < info.columnExprs.size(); ++i) {
            if (info.isReturnColumnExprs[i]) {
                schema->insertToGroupAndScope(info.columnExprs[i], groupPos);
            }
        }
        if (info.tableType == TableType::NODE) {
            auto node = ku_dynamic_cast<Expression*, NodeExpression*>(info.pattern.get());
            schema->insertToGroupAndScopeMayRepeat(node->getInternalID(), groupPos);
        }
    }
}

void LogicalInsert::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        for (auto i = 0u; i < info.columnExprs.size(); ++i) {
            if (info.isReturnColumnExprs[i]) {
                schema->insertToGroupAndScope(info.columnExprs[i], 0);
            }
        }
        if (info.tableType == TableType::NODE) {
            auto node = ku_dynamic_cast<Expression*, NodeExpression*>(info.pattern.get());
            schema->insertToGroupAndScopeMayRepeat(node->getInternalID(), 0);
        }
    }
}

std::string LogicalInsert::getExpressionsForPrinting() const {
    std::string result;
    for (auto i = 0u; i < infos.size() - 1; ++i) {
        result += infos[i].pattern->toString() + ",";
    }
    result += infos[infos.size() - 1].pattern->toString();
    return result;
}

f_group_pos_set LogicalInsert::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(childSchema->getGroupsPosInScope(),
        childSchema);
}

} // namespace planner
} // namespace kuzu
