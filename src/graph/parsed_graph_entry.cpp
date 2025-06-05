#include "graph/parsed_graph_entry.h"
#include "common/string_format.h"

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


}
}
