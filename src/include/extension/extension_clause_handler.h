#pragma once

#include "binder/binder.h"
#include "parser/statement.h"
#include "planner/operator/logical_plan.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace extension {

class ExtensionClause;
class BoundExtensionClause;
class LogicalExtensionClause;

using parse_func_t =
    std::function<std::vector<std::shared_ptr<parser::Statement>>(std::string_view)>;
using read_write_analyzer_t = std::function<bool(const extension::ExtensionClause&)>;
using bind_func_t = std::function<std::unique_ptr<binder::BoundStatement>(
    const extension::ExtensionClause&, const binder::Binder& binder)>;
using plan_func_t =
    std::function<void(planner::LogicalPlan&, const extension::BoundExtensionClause&)>;
using map_func_t = std::function<std::unique_ptr<processor::PhysicalOperator>(
    const extension::LogicalExtensionClause&, uint32_t operatorID)>;

inline bool defaultReadWriteAnalyzer(const extension::ExtensionClause& /*statement*/) {
    // Extension statements are readonly by default.
    return true;
}

struct ExtensionClauseHandler {
    ExtensionClauseHandler(parse_func_t parseFunc, bind_func_t bindFunc, plan_func_t planFunc,
        map_func_t mapFunc)
        : parseFunc{parseFunc}, bindFunc{bindFunc}, planFunc{planFunc}, mapFunc{mapFunc} {}

    parse_func_t parseFunc;
    bind_func_t bindFunc;
    plan_func_t planFunc;
    map_func_t mapFunc;
    read_write_analyzer_t readWriteAnalyzer = defaultReadWriteAnalyzer;
};

} // namespace extension
} // namespace kuzu
