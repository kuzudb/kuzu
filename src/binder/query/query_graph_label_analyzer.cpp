#include "binder/query/query_graph_label_analyzer.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_format.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace binder {

// NOLINTNEXTLINE(readability-non-const-parameter): graph is supposed to be modified.
void QueryGraphLabelAnalyzer::pruneLabel(QueryGraph& graph) const {
    for (auto i = 0u; i < graph.getNumQueryNodes(); ++i) {
        pruneNode(graph, *graph.getQueryNode(i));
    }
    for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
        pruneRel(*graph.getQueryRel(i));
    }
}

void QueryGraphLabelAnalyzer::pruneNode(const QueryGraph& graph, NodeExpression& node) const {
    auto catalog = clientContext.getCatalog();
    for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
        auto queryRel = graph.getQueryRel(i);
        if (queryRel->isRecursive()) {
            continue;
        }
        table_id_set_t candidates;
        std::unordered_set<std::string> candidateNamesSet;
        auto isSrcConnect = *queryRel->getSrcNode() == node;
        auto isDstConnect = *queryRel->getDstNode() == node;
        auto tx = clientContext.getTransaction();
        if (queryRel->getDirectionType() == RelDirectionType::BOTH) {
            if (isSrcConnect || isDstConnect) {
                for (auto entry : queryRel->getEntries()) {
                    auto& relEntry = entry->constCast<RelTableCatalogEntry>();
                    auto srcTableID = relEntry.getSrcTableID();
                    auto dstTableID = relEntry.getDstTableID();
                    candidates.insert(srcTableID);
                    candidates.insert(dstTableID);
                    auto srcEntry = catalog->getTableCatalogEntry(tx, srcTableID);
                    auto dstEntry = catalog->getTableCatalogEntry(tx, dstTableID);
                    candidateNamesSet.insert(srcEntry->getName());
                    candidateNamesSet.insert(dstEntry->getName());
                }
            }
        } else {
            if (isSrcConnect) {
                for (auto entry : queryRel->getEntries()) {
                    auto& relEntry = entry->constCast<RelTableCatalogEntry>();
                    auto srcTableID = relEntry.getSrcTableID();
                    candidates.insert(srcTableID);
                    auto srcEntry = catalog->getTableCatalogEntry(tx, srcTableID);
                    candidateNamesSet.insert(srcEntry->getName());
                }
            } else if (isDstConnect) {
                for (auto entry : queryRel->getEntries()) {
                    auto& relEntry = entry->constCast<RelTableCatalogEntry>();
                    auto dstTableID = relEntry.getDstTableID();
                    candidates.insert(dstTableID);
                    auto dstEntry = catalog->getTableCatalogEntry(tx, dstTableID);
                    candidateNamesSet.insert(dstEntry->getName());
                }
            }
        }
        if (candidates.empty()) { // No need to prune.
            continue;
        }
        std::vector<TableCatalogEntry*> prunedEntries;
        for (auto entry : node.getEntries()) {
            if (!candidates.contains(entry->getTableID())) {
                continue;
            }
            prunedEntries.push_back(entry);
        }
        node.setEntries(prunedEntries);
        if (prunedEntries.empty()) {
            if (throwOnViolate) {
                auto candidateNames =
                    std::vector<std::string>{candidateNamesSet.begin(), candidateNamesSet.end()};
                auto candidateStr = candidateNames[0];
                for (auto j = 1u; j < candidateNames.size(); ++j) {
                    candidateStr += ", " + candidateNames[j];
                }
                throw BinderException(
                    stringFormat("Query node {} violates schema. Expected labels are {}.",
                        node.toString(), candidateStr));
            }
        }
    }
}

std::vector<TableCatalogEntry*> QueryGraphLabelAnalyzer::pruneNonRecursiveRel(
    const std::vector<TableCatalogEntry*>& relEntries, const table_id_set_t& srcTableIDSet,
    const table_id_set_t& dstTableIDSet, const RelDirectionType directionType) const {

    auto forwardPruningFunc = [&](table_id_t srcTableID, table_id_t dstTableID) {
        return srcTableIDSet.contains(srcTableID) && dstTableIDSet.contains(dstTableID);
    };
    std::vector<TableCatalogEntry*> prunedEntries;
    for (auto& entry : relEntries) {
        auto& relEntry = entry->constCast<RelTableCatalogEntry>();
        auto srcTableID = relEntry.getSrcTableID();
        auto dstTableID = relEntry.getDstTableID();
        auto satisfyForwardPruning = forwardPruningFunc(srcTableID, dstTableID);
        if (directionType == RelDirectionType::BOTH) {
            if (satisfyForwardPruning ||
                (dstTableIDSet.contains(srcTableID) && srcTableIDSet.contains(dstTableID))) {
                prunedEntries.push_back(entry);
            }
        } else {
            if (satisfyForwardPruning) {
                prunedEntries.push_back(entry);
            }
        }
    }
    return prunedEntries;
}

table_id_set_t QueryGraphLabelAnalyzer::collectRelNodes(const RelDataDirection direction,
    std::vector<TableCatalogEntry*> relEntries) const {
    table_id_set_t nodeIDs;
    for (const auto& entry : relEntries) {
        const auto& relEntry = entry->constCast<RelTableCatalogEntry>();
        if (direction == RelDataDirection::FWD) {
            nodeIDs.insert(relEntry.getDstTableID());
        } else if (direction == RelDataDirection::BWD) {
            nodeIDs.insert(relEntry.getSrcTableID());
        } else {
            KU_UNREACHABLE;
        }
    }
    return nodeIDs;
}

std::pair<std::vector<table_id_set_t>, std::vector<table_id_set_t>>
QueryGraphLabelAnalyzer::pruneRecursiveRel(const std::vector<TableCatalogEntry*>& relEntries,
    const table_id_set_t srcTableIDSet, const table_id_set_t dstTableIDSet, size_t lowerBound,
    size_t upperBound, RelDirectionType relDirectionType) const {
    // src-->[dst,[rels]]
    std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>
        stepFromLeftGraph;
    std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>
        stepFromRightGraph;
    for (auto entry : relEntries) {
        auto& relEntry = entry->constCast<RelTableCatalogEntry>();
        auto srcTableID = relEntry.getSrcTableID();
        auto dstTableID = relEntry.getDstTableID();
        stepFromLeftGraph[srcTableID][dstTableID].push_back(relEntry.getTableID());
        stepFromRightGraph[dstTableID][srcTableID].push_back(relEntry.getTableID());
        if (relDirectionType == RelDirectionType::BOTH) {
            stepFromLeftGraph[dstTableID][srcTableID].push_back(relEntry.getTableID());
            stepFromRightGraph[srcTableID][dstTableID].push_back(relEntry.getTableID());
        }
    }
    auto stepFromLeft =
        pruneRecursiveRel(stepFromLeftGraph, srcTableIDSet, dstTableIDSet, lowerBound, upperBound);
    auto stepFromRight =
        pruneRecursiveRel(stepFromRightGraph, dstTableIDSet, srcTableIDSet, lowerBound, upperBound);
    return {stepFromLeft, stepFromRight};
}

std::vector<table_id_set_t> QueryGraphLabelAnalyzer::pruneRecursiveRel(
    const std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>& graph,
    const table_id_set_t& startTableIDSet, const table_id_set_t& endTableIDSet, size_t lowerBound,
    size_t upperBound) const {

    // There may be multiple rel type between src and dst
    using PATH = std::vector<table_id_vector_t>;
    std::vector<std::vector<PATH>> paths(upperBound + 1);

    std::function<void(table_id_t, size_t, std::vector<table_id_vector_t>&)> dfs;
    dfs = [&](table_id_t curTableID, size_t currentLength, std::vector<table_id_vector_t>& path) {
        if (currentLength >= lowerBound && currentLength <= upperBound &&
            endTableIDSet.contains(curTableID)) {
            paths[currentLength].push_back(path);
        }
        if (currentLength >= upperBound) {
            return;
        }
        if (graph.contains(curTableID)) {
            for (const auto& [neighbor, relTableID] : graph.at(curTableID)) {
                path.push_back(relTableID);
                dfs(neighbor, currentLength + 1, path);
                path.pop_back();
            }
        }
    };
    for (auto startTableID : startTableIDSet) {
        std::vector<table_id_vector_t> path;
        dfs(startTableID, 0, path);
    }
    // merge
    std::vector<table_id_set_t> stepActiveTableIDs(upperBound);
    for (auto i = lowerBound; i <= upperBound; ++i) {
        for (auto path : paths[i]) {
            for (auto j = 0u; j < i; ++j) {
                auto rels = path[j];
                stepActiveTableIDs[j].insert(rels.begin(), rels.end());
            }
        }
    }
    return stepActiveTableIDs;
}

std::vector<TableCatalogEntry*> QueryGraphLabelAnalyzer::getTableCatalogEntries(
    table_id_set_t tableIDs) const {
    std::vector<TableCatalogEntry*> relEntries;
    for (const auto& tableID : tableIDs) {
        relEntries.push_back(catalog->getTableCatalogEntry(tx, tableID));
    }
    return relEntries;
}

std::vector<table_id_t> QueryGraphLabelAnalyzer::getNodeTableIDs() const {
    std::vector<table_id_t> nodeTableIDs;
    for (auto node_table_entry : catalog->getNodeTableEntries(tx)) {
        nodeTableIDs.push_back(node_table_entry->getTableID());
    }
    return nodeTableIDs;
}

std::unordered_set<TableCatalogEntry*> QueryGraphLabelAnalyzer::mergeTableIDs(
    const std::vector<table_id_set_t>& v1, const std::vector<table_id_set_t>& v2) const {
    std::unordered_set<table_id_t> temp;
    for (auto tableIDs : v1) {
        temp.insert(tableIDs.begin(), tableIDs.end());
    }
    for (auto tableIDs : v2) {
        temp.insert(tableIDs.begin(), tableIDs.end());
    }
    std::unordered_set<TableCatalogEntry*> ans;
    for (table_id_t tableID : temp) {
        ans.emplace(catalog->getTableCatalogEntry(tx, tableID));
    }
    return ans;
}

static std::vector<catalog::TableCatalogEntry*> intersectEntries(
    std::vector<catalog::TableCatalogEntry*> v1, std::vector<catalog::TableCatalogEntry*> v2) {
    std::sort(v1.begin(), v1.end());
    std::sort(v2.begin(), v2.end());
    std::vector<catalog::TableCatalogEntry*> intersection;
    std::set_intersection(v1.begin(), v1.end(), v2.begin(), v2.end(),
        std::back_inserter(intersection));
    return intersection;
}

static bool isSameTableCatalogEntryVector(std::vector<TableCatalogEntry*> v1,
    std::vector<TableCatalogEntry*> v2) {
    auto compareFunc = [](TableCatalogEntry* a, TableCatalogEntry* b) {
        return a->getTableID() < b->getTableID();
    };
    std::sort(v1.begin(), v1.end(), compareFunc);
    std::sort(v2.begin(), v2.end(), compareFunc);
    return std::equal(v1.begin(), v1.end(), v2.begin(), v2.end());
}

void QueryGraphLabelAnalyzer::pruneRel(RelExpression& rel) const {
    auto srcTableIDSet = rel.getSrcNode()->getTableIDsSet();
    auto dstTableIDSet = rel.getDstNode()->getTableIDsSet();
    if (rel.isRecursive()) {
        auto nodeTableIDs = getNodeTableIDs();
        // there is no label on both sides
        if (rel.getUpperBound() == 0 || (srcTableIDSet.size() == dstTableIDSet.size() &&
                                            dstTableIDSet.size() == nodeTableIDs.size())) {
            return;
        }

        auto [stepFromLeftTableIDs, stepFromRightTableIDs] =
            pruneRecursiveRel(rel.getEntries(), srcTableIDSet, dstTableIDSet, rel.getLowerBound(),
                rel.getUpperBound(), rel.getDirectionType());
        auto recursiveInfo = rel.getRecursiveInfo();
        recursiveInfo->stepFromLeftActivationRelInfos = stepFromLeftTableIDs;
        recursiveInfo->stepFromRightActivationRelInfos = stepFromRightTableIDs;
        // todo we need reset rel entries?
        auto temp = mergeTableIDs(stepFromLeftTableIDs, stepFromRightTableIDs);
        std::vector<TableCatalogEntry*> newRelEntries{temp.begin(), temp.end()};
        if (!isSameTableCatalogEntryVector(newRelEntries, rel.getEntries())) {
            rel.setEntries(newRelEntries);
            recursiveInfo->rel->setEntries(newRelEntries);
            // update src&dst entries
            auto forwardRelNodes = collectRelNodes(RelDataDirection::BWD,
                getTableCatalogEntries(stepFromLeftTableIDs.front()));

            std::unordered_set<table_id_t> backwardRelNodes;
            for (auto i = rel.getLowerBound(); i <= rel.getUpperBound(); ++i) {
                if (i == 0) {
                    continue;
                }
                const auto relSrcNodes = collectRelNodes(RelDataDirection::FWD,
                    getTableCatalogEntries(stepFromLeftTableIDs.at(i - 1)));
                backwardRelNodes.insert(relSrcNodes.begin(), relSrcNodes.end());
            }

            if (rel.getDirectionType() == RelDirectionType::BOTH) {
                forwardRelNodes.insert(backwardRelNodes.begin(), backwardRelNodes.end());
                backwardRelNodes = forwardRelNodes;
            }

            auto newSrcNodeEntries = intersectEntries(rel.getSrcNode()->getEntries(),
                getTableCatalogEntries({forwardRelNodes.begin(), forwardRelNodes.end()}));
            rel.getSrcNode()->setEntries(newSrcNodeEntries);

            auto newDstNodeEntries = intersectEntries(rel.getDstNode()->getEntries(),
                getTableCatalogEntries({backwardRelNodes.begin(), backwardRelNodes.end()}));
            rel.getDstNode()->setEntries(newDstNodeEntries);
        }
    } else {
        auto prunedEntries = pruneNonRecursiveRel(rel.getEntries(), srcTableIDSet, dstTableIDSet,
            rel.getDirectionType());
        rel.setEntries(prunedEntries);
    }
    // Note the pruning for node should guarantee the following exception won't be triggered.
    // For safety (and consistency) reason, we still write the check but skip coverage check.
    // LCOV_EXCL_START
    if (rel.getEntries().empty()) {
        if (throwOnViolate) {
            throw BinderException(stringFormat("Cannot find a label for relationship {} that "
                                               "connects to all of its neighbour nodes.",
                rel.toString()));
        }
    }
    // LCOV_EXCL_STOP
}

} // namespace binder
} // namespace kuzu
