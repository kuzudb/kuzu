#include "common/exception/parser.h"
#include "parser/project_graph.h"
#include "parser/transformer.h"

namespace kuzu {
namespace parser {

std::unique_ptr<ProjectGraph> Transformer::transformProjectGraph(
    CypherParser::KU_ProjectGraphContext& ctx) {
    auto subgraphName = transformSchemaName(*ctx.oC_SchemaName());
    std::vector<std::string> tableNames;
    for (auto tableItem : ctx.kU_GraphProjectionTableItems()->kU_GraphProjectionTableItem()) {
        if (tableItem->kU_GraphProjectionColumnItems() != nullptr) {
            throw common::ParserException(
                "Filtering or projecting properties in graph projection is not supported.");
        }
        tableNames.push_back(transformSchemaName(*tableItem->oC_SchemaName()));
    }
    if (ctx.IN_MEM()) {
        return std::make_unique<ProjectGraph>(std::move(subgraphName), std::move(tableNames), true);
    } else {
        return std::make_unique<ProjectGraph>(std::move(subgraphName), std::move(tableNames), false);
    }
}

} // namespace parser
} // namespace kuzu
