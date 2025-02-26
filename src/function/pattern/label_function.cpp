#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/expression_binder.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "function/binary_function_executor.h"
#include "function/list/functions/list_extract_function.h"
#include "function/rewrite_function.h"
#include "function/scalar_function.h"
#include "function/schema/vector_node_rel_functions.h"
#include "function/struct/vector_struct_functions.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct Label {
    static inline void operation(common::internalID_t& left, common::list_entry_t& right,
        common::ku_string_t& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& resultVector, uint64_t resPos) {
        KU_ASSERT(left.tableID < right.size);
        ListExtract::operation(right, left.tableID + 1 /* listExtract requires 1-based index */,
            result, rightVector, leftVector, resultVector, resPos);
    }
};

static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
    const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* dataPtr = nullptr) {
    KU_ASSERT(params.size() == 2);
    BinaryFunctionExecutor::executeSwitch<common::internalID_t, common::list_entry_t,
        common::ku_string_t, Label, BinaryListExtractFunctionWrapper>(*params[0],
        paramSelVectors[0], *params[1], paramSelVectors[1], result, resultSelVector, dataPtr);
}

static std::shared_ptr<binder::Expression> getLabelsAsLiteral(main::ClientContext* context,
    std::vector<TableCatalogEntry*> entries, binder::ExpressionBinder* expressionBinder) {
    std::unordered_map<table_id_t, std::string> map;
    table_id_t maxTableID = 0;
    for (auto& entry : entries) {
        map.insert({entry->getTableID(),
            entry->getLabel(context->getCatalog(), context->getTransaction())});
        if (entry->getTableID() > maxTableID) {
            maxTableID = entry->getTableID();
        }
    }
    std::vector<std::unique_ptr<Value>> labels;
    labels.resize(maxTableID + 1);
    for (auto i = 0u; i < labels.size(); ++i) {
        if (map.contains(i)) {
            labels[i] = std::make_unique<Value>(LogicalType::STRING(), map.at(i));
        } else {
            labels[i] = std::make_unique<Value>(LogicalType::STRING(), std::string(""));
        }
    }
    auto labelsValue = Value(LogicalType::LIST(LogicalType::STRING()), std::move(labels));
    return expressionBinder->createLiteralExpression(labelsValue);
}

std::shared_ptr<Expression> LabelFunction::rewriteFunc(const RewriteFunctionBindInput& input) {
    KU_ASSERT(input.arguments.size() == 1);
    auto argument = input.arguments[0].get();
    auto expressionBinder = input.expressionBinder;
    auto context = input.context;
    expression_vector children;
    if (argument->expressionType == ExpressionType::VARIABLE) {
        children.push_back(input.arguments[0]);
        children.push_back(expressionBinder->createLiteralExpression(InternalKeyword::LABEL));
        return expressionBinder->bindScalarFunctionExpression(children,
            StructExtractFunctions::name);
    }
    auto disableLiteralRewrite = expressionBinder->getConfig().disableLabelFunctionLiteralRewrite;
    if (ExpressionUtil::isNodePattern(*argument)) {
        auto& node = argument->constCast<NodeExpression>();
        if (!disableLiteralRewrite) {
            if (node.isEmpty()) {
                return expressionBinder->createLiteralExpression("");
            }
            if (!node.isMultiLabeled()) {
                auto label = node.getSingleEntry()->getLabel(context->getCatalog(),
                    context->getTransaction());
                return expressionBinder->createLiteralExpression(label);
            }
        }
        children.push_back(node.getInternalID());
        children.push_back(getLabelsAsLiteral(context, node.getEntries(), expressionBinder));
    } else if (ExpressionUtil::isRelPattern(*argument)) {
        auto& rel = argument->constCast<RelExpression>();
        if (!disableLiteralRewrite) {
            if (rel.isEmpty()) {
                return expressionBinder->createLiteralExpression("");
            }
            if (!rel.isMultiLabeled()) {
                auto label = rel.getSingleEntry()->getLabel(context->getCatalog(),
                    context->getTransaction());
                return expressionBinder->createLiteralExpression(label);
            }
        }
        children.push_back(rel.getInternalIDProperty());
        children.push_back(getLabelsAsLiteral(context, rel.getEntries(), expressionBinder));
    }
    KU_ASSERT(children.size() == 2);
    auto function = std::make_unique<ScalarFunction>(LabelFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, execFunction);
    auto bindData = std::make_unique<function::FunctionBindData>(LogicalType::STRING());
    auto uniqueName = ScalarFunctionExpression::getUniqueName(LabelFunction::name, children);
    return std::make_shared<ScalarFunctionExpression>(ExpressionType::FUNCTION, std::move(function),
        std::move(bindData), std::move(children), uniqueName);
}

function_set LabelFunction::getFunctionSet() {
    function_set set;
    auto inputTypes =
        std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT};
    for (auto& inputType : inputTypes) {
        auto function = std::make_unique<RewriteFunction>(name,
            std::vector<LogicalTypeID>{inputType}, rewriteFunc);
        set.push_back(std::move(function));
    }
    return set;
}

} // namespace function
} // namespace kuzu
