#pragma once

#include "common/enums/rel_direction.h"
#include "main/client_context.h"
#include "query_graph.h"
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    explicit QueryGraphLabelAnalyzer(const main::ClientContext& clientContext, bool throwOnViolate)
        : throwOnViolate{throwOnViolate}, clientContext{clientContext} {
        tx = clientContext.getTx();
        catalog = clientContext.getCatalog();
    }

    void pruneLabel(QueryGraph& graph);

private:
    class PrunedEntries {
    public:
        explicit PrunedEntries(size_t size) : prunedEntries{}, indexVector{} {
            indexVector.resize(size);
        }

        const std::vector<TableCatalogEntry*>& getPrunedEntries(size_t index) const {
            return prunedEntries.at(indexVector[index]);
        }

        void insert(std::vector<TableCatalogEntry*> entries, size_t index) {
            indexVector[index] = prunedEntries.size();
            prunedEntries.emplace_back(entries);
        }

        // include&exclude
        PrunedEntries sub(size_t from, size_t to) const {
            PrunedEntries subPrunedEntries(to - from);
            size_t j = 0;
            for (auto i = from; i < to; ++i) {
                subPrunedEntries.insert(getPrunedEntries(i), j++);
            }
            return subPrunedEntries;
        }

        void unionPrunedEntries(const PrunedEntries other) {
            KU_ASSERT(indexVector.size() >= other.indexVector.size());
            for (auto i = 0u; i < other.indexVector.size(); ++i) {
                auto& entries = prunedEntries.at(indexVector[i]);
                auto& otherEntries = other.getPrunedEntries(i);
                appendEntriesWithDistinct(entries, otherEntries);
            }
        }

        void insert(size_t entryIndex, size_t index) { indexVector[index] = entryIndex; }
        size_t getEntryIndex(size_t index) const { return indexVector[index]; }
        void reverse() { std::reverse(indexVector.begin(), indexVector.end()); }

        bool empty() const { return indexVector.empty(); }
        static PrunedEntries merge(const PrunedEntries p1, const PrunedEntries p2) {
            KU_ASSERT(p1.indexVector.size() == p2.indexVector.size());
            auto size = p1.indexVector.size();
            PrunedEntries ans(size);
            for (auto i = 0u; i < size; ++i) {
                auto vec1 = p1.getPrunedEntries(i);
                auto vec2 = p2.getPrunedEntries(i);
                auto intersection = intersectEntries(vec1, vec2);
                ans.insert(intersection, i);
            }
            return ans;
        }

        const std::vector<std::vector<TableCatalogEntry*>>& getPrunedEntries() const {
            return prunedEntries;
        }

    private:
        static void appendEntriesWithDistinct(std::vector<TableCatalogEntry*>& target,
            const std::vector<TableCatalogEntry*>& source) {
            std::unordered_set<TableCatalogEntry*> set{target.begin(), target.end()};
            set.insert(source.begin(), source.end());
            target.clear();
            std::copy(set.begin(), set.end(), std::back_inserter(target));
        }

    private:
        std::vector<std::vector<TableCatalogEntry*>> prunedEntries;
        std::vector<size_t> indexVector;
    };

    void pruneNode(const QueryGraph& graph, NodeExpression& node);
    void pruneRel(RelExpression& rel) const;

    common::table_id_set_t collectRelNodes(const common::RelDataDirection direction,
        std::vector<catalog::TableCatalogEntry*> relEntries) const;

    PrunedEntries pruneRecursiveRel(const std::vector<catalog::TableCatalogEntry*>& relEntries,
        common::table_id_set_t srcTableIDSet, common::table_id_set_t dstTableIDSet,
        const RelDirectionType directionType, size_t lowerBound, size_t upperBound,
        common::table_id_set_t allNodeTableIDs) const;

    std::vector<TableCatalogEntry*> pruneNonRecursiveRel(
        const std::vector<catalog::TableCatalogEntry*>& relEntries,
        const common::table_id_set_t& srcTableIDSet, const common::table_id_set_t& dstTableIDSet,
        const RelDirectionType directionType) const;

    std::vector<catalog::TableCatalogEntry*> getTableCatalogEntries(
        std::vector<common::table_id_t> tableIDs) const;

    static std::vector<TableCatalogEntry*> intersectEntries(std::vector<TableCatalogEntry*> target,
        std::vector<TableCatalogEntry*> source) {
        std::sort(target.begin(), target.end());
        std::sort(source.begin(), source.end());
        std::vector<TableCatalogEntry*> intersection;
        std::set_intersection(target.begin(), target.end(), source.begin(), source.end(),
            std::back_inserter(intersection));
        return intersection;
    }

private:
    bool throwOnViolate;
    const main::ClientContext& clientContext;
    Transaction* tx;
    Catalog* catalog;
};

} // namespace binder
} // namespace kuzu
