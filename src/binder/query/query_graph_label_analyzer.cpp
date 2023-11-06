#include "binder/query/query_graph_label_analyzer.h"

#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/string_format.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

void QueryGraphLabelAnalyzer::pruneLabel(const QueryGraph& graph) {
    for (auto i = 0u; i < graph.getNumQueryNodes(); ++i) {
        pruneNode(graph, *graph.getQueryNode(i));
    }
    for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
        pruneRel(*graph.getQueryRel(i));
    }
}

void QueryGraphLabelAnalyzer::pruneNode(const QueryGraph& graph, NodeExpression& node) {
    for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
        auto queryRel = graph.getQueryRel(i);
        if (queryRel->isRecursive()) {
            continue;
        }
        common::table_id_set_t candidates;
        auto isSrcConnect = *queryRel->getSrcNode() == node;
        auto isDstConnect = *queryRel->getDstNode() == node;
        if (queryRel->getDirectionType() == RelDirectionType::BOTH) {
            if (isSrcConnect || isDstConnect) {
                for (auto relTableID : queryRel->getTableIDs()) {
                    auto relTableSchema = reinterpret_cast<RelTableSchema*>(
                        catalog.getReadOnlyVersion()->getTableSchema(relTableID));
                    candidates.insert(relTableSchema->getSrcTableID());
                    candidates.insert(relTableSchema->getDstTableID());
                }
            }
        } else {
            if (isSrcConnect) {
                for (auto relTableID : queryRel->getTableIDs()) {
                    auto relTableSchema = reinterpret_cast<RelTableSchema*>(
                        catalog.getReadOnlyVersion()->getTableSchema(relTableID));
                    candidates.insert(relTableSchema->getSrcTableID());
                }
            } else if (isDstConnect) {
                for (auto relTableID : queryRel->getTableIDs()) {
                    auto relTableSchema = reinterpret_cast<RelTableSchema*>(
                        catalog.getReadOnlyVersion()->getTableSchema(relTableID));
                    candidates.insert(relTableSchema->getDstTableID());
                }
            }
        }
        if (candidates.empty()) {
            return;
        }
        common::table_id_vector_t prunedTableIDs;
        for (auto tableID : node.getTableIDs()) {
            if (!candidates.contains(tableID)) {
                continue;
            }
            prunedTableIDs.push_back(tableID);
        }
        if (prunedTableIDs.empty()) {
            throw BinderException(stringFormat("Cannot find a label for node {} that connects to "
                                               "all of its neighbour relationships.",
                node.toString()));
        }
        node.setTableIDs(std::move(prunedTableIDs));
    }
}

void QueryGraphLabelAnalyzer::pruneRel(RelExpression& rel) {
    if (rel.isRecursive()) {
        return;
    }
    common::table_id_vector_t prunedTableIDs;
    if (rel.getDirectionType() == RelDirectionType::BOTH) {
        common::table_id_set_t boundTableIDSet;
        for (auto tableID : rel.getSrcNode()->getTableIDs()) {
            boundTableIDSet.insert(tableID);
        }
        for (auto tableID : rel.getDstNode()->getTableIDs()) {
            boundTableIDSet.insert(tableID);
        }
        for (auto& relTableID : rel.getTableIDs()) {
            auto relTableSchema = reinterpret_cast<RelTableSchema*>(
                catalog.getReadOnlyVersion()->getTableSchema(relTableID));
            auto srcTableID = relTableSchema->getSrcTableID();
            auto dstTableID = relTableSchema->getDstTableID();
            if (!boundTableIDSet.contains(srcTableID) || !boundTableIDSet.contains(dstTableID)) {
                continue;
            }
            prunedTableIDs.push_back(relTableID);
        }
    } else {
        auto srcTableIDSet = rel.getSrcNode()->getTableIDsSet();
        auto dstTableIDSet = rel.getDstNode()->getTableIDsSet();
        for (auto& relTableID : rel.getTableIDs()) {
            auto relTableSchema = reinterpret_cast<RelTableSchema*>(
                catalog.getReadOnlyVersion()->getTableSchema(relTableID));
            auto srcTableID = relTableSchema->getSrcTableID();
            auto dstTableID = relTableSchema->getDstTableID();
            if (!srcTableIDSet.contains(srcTableID) || !dstTableIDSet.contains(dstTableID)) {
                continue;
            }
            prunedTableIDs.push_back(relTableID);
        }
    }
    // Note the pruning for node should guarantee the following exception won't be triggered.
    // For safety (and consistency) reason, we still write the check but skip coverage check.
    // LCOV_EXCL_START
    if (prunedTableIDs.empty()) {
        throw BinderException(stringFormat(
            "Cannot find a label for relationship {} that connects to all of its neighbour nodes.",
            rel.toString()));
    }
    // LCOV_EXCL_STOP
    rel.setTableIDs(std::move(prunedTableIDs));
}

} // namespace binder
} // namespace kuzu
