#include "function/gds/gds.h"

#include "binder/binder.h"
#include "common/exception/binder.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

graph::GraphEntry GDSAlgorithm::bindGraphEntry(main::ClientContext& context,
    const std::string& name) {
    if (!context.getGraphEntrySetUnsafe().hasGraph(name)) {
        throw BinderException(stringFormat("Cannot find graph {}.", name));
    }
    return context.getGraphEntrySetUnsafe().getEntry(name);
}

std::shared_ptr<Expression> GDSAlgorithm::bindNodeOutput(const GDSBindInput& bindInput,
    const std::vector<catalog::TableCatalogEntry*>& nodeEntries) {
    std::string nodeColumnName = NODE_COLUMN_NAME;
    if (!bindInput.yieldVariables.empty()) {
        nodeColumnName = bindColumnName(bindInput.yieldVariables[0], nodeColumnName);
    }
    auto node = bindInput.binder->createQueryNode(nodeColumnName, nodeEntries);
    bindInput.binder->addToScope(nodeColumnName, node);
    return node;
}

std::string GDSAlgorithm::bindColumnName(const parser::YieldVariable& yieldVariable,
    std::string expressionName) {
    if (yieldVariable.name != expressionName) {
        throw common::BinderException{
            common::stringFormat("Unknown variable name: {}.", yieldVariable.name)};
    }
    return yieldVariable.hasAlias() ? yieldVariable.alias : expressionName;
}

} // namespace function
} // namespace kuzu
