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
    auto backwardPruningFunc = [&](table_id_t srcTableID, table_id_t dstTableID) {
        return dstTableIDSet.contains(srcTableID) && srcTableIDSet.contains(dstTableID);
    };
    std::vector<TableCatalogEntry*> prunedEntries;
    for (auto& entry : relEntries) {
        auto& relEntry = entry->constCast<RelTableCatalogEntry>();
        auto srcTableID = relEntry.getSrcTableID();
        auto dstTableID = relEntry.getDstTableID();
        auto satisfyForwardPruning = forwardPruningFunc(srcTableID, dstTableID);
        if (directionType == RelDirectionType::BOTH) {
            if (satisfyForwardPruning || backwardPruningFunc(srcTableID, dstTableID)) {
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
    table_id_t maxTableID = 0;
    for (auto entry : relEntries) {
        auto& relEntry = entry->constCast<RelTableCatalogEntry>();
        auto srcTableID = relEntry.getSrcTableID();
        auto dstTableID = relEntry.getDstTableID();
        auto tableID = relEntry.getTableID();
        stepFromLeftGraph[srcTableID][dstTableID].push_back(tableID);
        stepFromRightGraph[dstTableID][srcTableID].push_back(tableID);
        if (relDirectionType == RelDirectionType::BOTH) {
            stepFromLeftGraph[dstTableID][srcTableID].push_back(tableID);
            stepFromRightGraph[srcTableID][dstTableID].push_back(tableID);
        }
        maxTableID = std::max(maxTableID, tableID);
    }

    auto stepFromLeft = pruneRecursiveRel(stepFromLeftGraph, stepFromRightGraph, srcTableIDSet,
        dstTableIDSet, lowerBound, upperBound, maxTableID);
    auto stepFromRight = pruneRecursiveRel(stepFromRightGraph, stepFromLeftGraph, dstTableIDSet,
        srcTableIDSet, lowerBound, upperBound, maxTableID);
    return {stepFromLeft, stepFromRight};
}

std::vector<table_id_set_t> QueryGraphLabelAnalyzer::pruneRecursiveRel(
    const std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>& graph,
    const std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>&
        reseveGraph,
    const table_id_set_t& startTableIDSet, const table_id_set_t& endTableIDSet, size_t lowerBound,
    size_t upperBound, table_id_t maxTableID) const {

    // f[i][j] represent whether the edge numbered i can be reached by jumping j times through the
    // set A.
    std::unordered_map<table_id_t, std::vector<bool>> f, g;

    auto initFunc = [upperBound](const std::unordered_map<table_id_t,
                                     std::unordered_map<table_id_t, table_id_vector_t>>& _graph,
                        std::unordered_map<table_id_t, std::vector<bool>>& ans,
                        const table_id_set_t& beginTableIDSet) {
        for (auto [_, map] : _graph) {
            for (auto [_, rels] : map) {
                for (auto rel : rels) {
                    ans.emplace(rel, std::vector<bool>(upperBound + 1, false));
                }
            }
        }

        for (auto tableID : beginTableIDSet) {
            if (!_graph.contains(tableID)) {
                continue;
            }
            for (auto [dst, rels] : _graph.at(tableID)) {
                for (auto rel : rels) {
                    ans[rel][1] = true;
                    ans[rel][0] = true;
                }
            }
        }
    };

    initFunc(graph, f, startTableIDSet);
    initFunc(reseveGraph, g, endTableIDSet);

    auto isOk = [&](const table_id_vector_t& rels, int j,
                    std::unordered_map<table_id_t, std::vector<bool>>& map) -> bool {
        for (auto rel : rels) {
            if (map[rel][j - 1]) {
                return true;
            }
        }
        return false;
    };

    auto bfsFunc =
        [upperBound, maxTableID, isOk](
            const std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>&
                _graph,
            const std::unordered_map<table_id_t, std::unordered_map<table_id_t, table_id_vector_t>>&
                _reseveGraph,
            std::unordered_map<table_id_t, std::vector<bool>>& map) {
            for (int j = 2; j <= upperBound; ++j) {
                for (auto v = 0u; v < maxTableID; ++v) {
                    bool flag = false;
                    if (_reseveGraph.contains(v)) {
                        for (auto [_, rels] : _reseveGraph.at(v)) {
                            if (isOk(rels, j, map)) {
                                flag = true;
                                break;
                            }
                        }
                    }

                    if (flag && _graph.contains(v)) {
                        for (auto [dst, rels] : _graph.at(v)) {
                            for (auto rel : rels) {
                                map[rel][j] = true;
                            }
                        }
                    }
                }
            }
        };

    bfsFunc(graph, reseveGraph, f);
    bfsFunc(reseveGraph, graph, g);

    std::vector<table_id_set_t> stepActiveTableIDs(upperBound);
    for (auto [rel, vector] : f) {
        for (int j = 0; j <= upperBound; ++j) {
            if (!vector[j]) {
                continue;
            }
            for (int k = 0; k <= upperBound; ++k) {
                if (!g[rel][k]) {
                    continue;
                }
                auto step = j + k;
                if (step != upperBound) {
                    // rel repeat count
                    step--;
                }
                if (step < lowerBound) {
                    continue;
                } else if (step > upperBound) {
                    break;
                } else {
                    int index = j == 0 ? 0 : j - 1;
                    stepActiveTableIDs[index].emplace(rel);
                    break;
                }
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
        if (rel.getUpperBound() == 0 || rel.getEntries().empty()) {
            return;
        }

        auto [stepFromLeftTableIDs, stepFromRightTableIDs] =
            pruneRecursiveRel(rel.getEntries(), srcTableIDSet, dstTableIDSet, rel.getLowerBound(),
                rel.getUpperBound(), rel.getDirectionType());
        auto recursiveInfo = rel.getRecursiveInfoUnsafe();
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
