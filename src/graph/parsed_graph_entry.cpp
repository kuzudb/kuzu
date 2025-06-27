#include "graph/parsed_graph_entry.h"

using namespace kuzu::common;

namespace kuzu {
namespace graph {

std::string GraphEntryTypeUtils::toString(GraphEntryType type) {
    switch (type) {
    case GraphEntryType::NATIVE:
        return "NATIVE";
    case GraphEntryType::CYPHER:
        return "CYPHER";
    default:
        KU_UNREACHABLE;
    }
}

std::string ParsedNativeGraphTableInfo::toString() const {
    auto result = common::stringFormat("{'table': '{}'", tableName);
    if (predicate != "") {
        result += common::stringFormat(",'predicate': '{}'", predicate);
    }
    result += "}";
    return result;
}

} // namespace graph
} // namespace kuzu
