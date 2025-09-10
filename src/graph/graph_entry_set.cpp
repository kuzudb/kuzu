#include "graph/graph_entry_set.h"

#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace graph {

void GraphEntrySet::validateGraphNotExist(const std::string& name) const {
    if (hasGraph(name)) {
        throw RuntimeException(stringFormat("Projected graph {} already exists.", name));
    }
}

void GraphEntrySet::validateGraphExist(const std::string& name) const {
    if (!hasGraph(name)) {
        throw RuntimeException(stringFormat("Projected graph {} does not exists.", name));
    }
}

GraphEntrySet* GraphEntrySet::Get(const main::ClientContext& context) {
    return context.graphEntrySet.get();
}

} // namespace graph
} // namespace kuzu
