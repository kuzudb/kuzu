#include "planner/planner.h"

#include "main/client_context.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace planner {

bool QueryGraphPlanningInfo::containsCorrExpr(const Expression& expr) const {
    for (auto& corrExpr : corrExprs) {
        if (*corrExpr == expr) {
            return true;
        }
    }
    return false;
}

expression_vector PropertyExprCollection::getProperties(const Expression& pattern) const {
    if (!patternNameToProperties.contains(pattern.getUniqueName())) {
        return binder::expression_vector{};
    }
    return patternNameToProperties.at(pattern.getUniqueName());
}

expression_vector PropertyExprCollection::getProperties() const {
    expression_vector result;
    for (auto& [_, exprs] : patternNameToProperties) {
        for (auto& expr : exprs) {
            result.push_back(expr);
        }
    }
    return result;
}

void PropertyExprCollection::addProperties(const std::string& patternName,
    std::shared_ptr<Expression> property) {
    if (!patternNameToProperties.contains(patternName)) {
        patternNameToProperties.insert({patternName, expression_vector{}});
    }
    for (auto& p : patternNameToProperties.at(patternName)) {
        if (*p == *property) {
            return;
        }
    }
    patternNameToProperties.at(patternName).push_back(property);
}

void PropertyExprCollection::clear() {
    patternNameToProperties.clear();
}

Planner::Planner(main::ClientContext* clientContext) : clientContext{clientContext} {
    cardinalityEstimator = CardinalityEstimator(clientContext);
    context = JoinOrderEnumeratorContext();
}

LogicalPlan Planner::planStatement(const BoundStatement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        return planQuery(statement);
    } break;
    case StatementType::CREATE_TABLE: {
        return planCreateTable(statement);
    } break;
    case StatementType::CREATE_SEQUENCE: {
        return planCreateSequence(statement);
    } break;
    case StatementType::CREATE_TYPE: {
        return planCreateType(statement);
    } break;
    case StatementType::COPY_FROM: {
        return planCopyFrom(statement);
    } break;
    case StatementType::COPY_TO: {
        return planCopyTo(statement);
    } break;
    case StatementType::DROP: {
        return planDrop(statement);
    } break;
    case StatementType::ALTER: {
        return planAlter(statement);
    } break;
    case StatementType::STANDALONE_CALL: {
        return planStandaloneCall(statement);
    } break;
    case StatementType::STANDALONE_CALL_FUNCTION: {
        return planStandaloneCallFunction(statement);
    } break;
    case StatementType::EXPLAIN: {
        return planExplain(statement);
    } break;
    case StatementType::CREATE_MACRO: {
        return planCreateMacro(statement);
    } break;
    case StatementType::TRANSACTION: {
        return planTransaction(statement);
    } break;
    case StatementType::EXTENSION: {
        return planExtension(statement);
    } break;
    case StatementType::EXPORT_DATABASE: {
        return planExportDatabase(statement);
    } break;
    case StatementType::IMPORT_DATABASE: {
        return planImportDatabase(statement);
    } break;
    case StatementType::ATTACH_DATABASE: {
        return planAttachDatabase(statement);
    } break;
    case StatementType::DETACH_DATABASE: {
        return planDetachDatabase(statement);
    } break;
    case StatementType::USE_DATABASE: {
        return planUseDatabase(statement);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace planner
} // namespace kuzu
