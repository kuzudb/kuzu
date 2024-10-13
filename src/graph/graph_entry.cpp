#include "graph/graph_entry.h"

#include "binder/expression/expression_util.h"
#include "binder/expression_visitor.h"
#include "planner/operator/schema.h"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace graph {

const catalog::TableCatalogEntry* GraphEntry::getRelEntry(common::table_id_t tableID) const {
    for (auto entry : relEntries) {
        if (entry->getTableID() == tableID) {
            return entry;
        }
    }
    return nullptr;
}

void GraphEntry::setRelPredicate(std::shared_ptr<binder::Expression> predicate) {
    relPredicate = predicate;
    auto collector = PropertyExprCollector();
    collector.visit(relPredicate);
    relProperties = ExpressionUtil::removeDuplication(collector.getPropertyExprs());
}

Schema GraphEntry::getRelPropertiesSchema() const {
    auto schema = Schema();
    schema.createGroup();
    for (auto expr : relProperties) {
        schema.insertToGroupAndScope(expr, 0);
    }
    return schema;
}

} // namespace graph
} // namespace kuzu
