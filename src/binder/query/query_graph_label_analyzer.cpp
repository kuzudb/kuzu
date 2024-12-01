#include "binder/query/query_graph_label_analyzer.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_format.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace binder {

void QueryGraphLabelAnalyzer::pruneLabel(QueryGraph& graph) {
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
        std::unordered_set<std::string> candidateNamesSet;
        auto isSrcConnect = *queryRel->getSrcNode() == node;
        auto isDstConnect = *queryRel->getDstNode() == node;
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
            return;
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
    const std::vector<catalog::TableCatalogEntry*>& relEntries,
    const common::table_id_set_t& srcTableIDSet, const common::table_id_set_t& dstTableIDSet,
    const RelDirectionType directionType) const {

    auto forwardPruningFunc = [&](common::table_id_t srcTableID, common::table_id_t dstTableID) {
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

common::table_id_set_t QueryGraphLabelAnalyzer::collectRelNodes(
    const common::RelDataDirection direction,
    std::vector<catalog::TableCatalogEntry*> relEntries) const {
    common::table_id_set_t nodeIDs;
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

QueryGraphLabelAnalyzer::PrunedEntries QueryGraphLabelAnalyzer::pruneRecursiveRel(
    const std::vector<catalog::TableCatalogEntry*>& relEntries,
    common::table_id_set_t srcTableIDSet, common::table_id_set_t dstTableIDSet,
    const RelDirectionType directionType, size_t lowerBound, size_t upperBound,
    common::table_id_set_t allNodeTableIDs) const {

    auto prunFunc =
        [this, upperBound, &relEntries, &directionType](common::table_id_set_t srcTableIDSet,
            common::table_id_set_t dstTableIDSet, const common::RelDataDirection direction) {
            PrunedEntries forwardPrunedEntries(upperBound);
            for (auto i = 0u; i < upperBound; ++i) {
                auto prunedEntries =
                    pruneNonRecursiveRel(relEntries, srcTableIDSet, dstTableIDSet, directionType);
                if (i != 0 && std::equal(prunedEntries.begin(), prunedEntries.end(),
                                  forwardPrunedEntries.getPrunedEntries(i - 1).begin())) {
                    for (auto j = i; j < upperBound; ++j) {
                        forwardPrunedEntries.insert(forwardPrunedEntries.getEntryIndex(j - 1), j);
                    }
                    break;
                } else {
                    if (direction == RelDataDirection::FWD) {
                        srcTableIDSet = collectRelNodes(direction, prunedEntries);
                    } else {
                        dstTableIDSet = collectRelNodes(direction, prunedEntries);
                    }
                    forwardPrunedEntries.insert(prunedEntries, i);
                }
            }

            if (direction == RelDataDirection::BWD) {
                forwardPrunedEntries.reverse();
            }
            return forwardPrunedEntries;
        };

    const auto& forwardPrunedEntries =
        prunFunc(srcTableIDSet, allNodeTableIDs, RelDataDirection::FWD);
    const auto& backwardPrunedEntries =
        prunFunc(allNodeTableIDs, dstTableIDSet, RelDataDirection::BWD);

    auto upperPE = PrunedEntries::merge(forwardPrunedEntries, backwardPrunedEntries);
    if (lowerBound != upperBound) {
        for (auto i = lowerBound; i < upperBound; ++i) {
            auto forwardSubPrunedEntries = forwardPrunedEntries.sub(0, i);
            auto backwardSubPrunedEntries = backwardPrunedEntries.sub(upperBound - i, upperBound);
            auto tempPrunedEntries =
                PrunedEntries::merge(forwardSubPrunedEntries, backwardSubPrunedEntries);
            upperPE.unionPrunedEntries(tempPrunedEntries);
        }
    }
    return upperPE;
}

std::vector<catalog::TableCatalogEntry*> QueryGraphLabelAnalyzer::getTableCatalogEntries(
    std::vector<common::table_id_t> tableIDs) const {
    std::vector<catalog::TableCatalogEntry*> relEntries;
    for (const auto& tableID : tableIDs) {
        relEntries.push_back(catalog->getTableCatalogEntry(tx, tableID));
    }
    return relEntries;
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
        auto nodeTableIDs = catalog->getNodeTableIDs(tx);
        // there is no label on both sides
        if (srcTableIDSet.size() == dstTableIDSet.size() &&
            dstTableIDSet.size() == nodeTableIDs.size()) {
            return;
        }

        common::table_id_set_t allNodeTableIDs{nodeTableIDs.begin(), nodeTableIDs.end()};
        auto prunedEntries = pruneRecursiveRel(rel.getEntries(), srcTableIDSet, dstTableIDSet,
            rel.getDirectionType(), rel.getLowerBound(), rel.getUpperBound(), allNodeTableIDs);

        if (!prunedEntries.empty()) {
            std::vector<std::vector<table_id_t>> stepActivationRelInfos;
            std::unordered_set<TableCatalogEntry*> temp;
            for (const auto& item : prunedEntries.getPrunedEntries()) {
                temp.insert(item.begin(), item.end());
                std::vector<table_id_t> activationTabelIDs;
                for (const auto& entry : item) {
                    activationTabelIDs.push_back(entry->getTableID());
                }
                stepActivationRelInfos.push_back(activationTabelIDs);
            }

            rel.getRecursiveInfo()->stepActivationRelInfos = stepActivationRelInfos;
            std::vector<catalog::TableCatalogEntry*> newRelEntries{temp.begin(), temp.end()};
            if (!isSameTableCatalogEntryVector(newRelEntries, rel.getEntries())) {
                rel.setEntries(newRelEntries);
                rel.getRecursiveInfo()->rel->setEntries(newRelEntries);
                // update src&dst entries
                auto forwardRelNodes = collectRelNodes(RelDataDirection::BWD,
                    getTableCatalogEntries(stepActivationRelInfos.front()));

                std::unordered_set<table_id_t> backwardRelNodes;
                for (auto i = rel.getLowerBound(); i <= rel.getUpperBound(); ++i) {
                    if (i == 0) {
                        continue;
                    }
                    const auto relSrcNodes = collectRelNodes(RelDataDirection::FWD,
                        getTableCatalogEntries(stepActivationRelInfos.at(i - 1)));
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
