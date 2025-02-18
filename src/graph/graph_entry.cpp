#include "graph/graph_entry.h"

#include "common/exception/runtime.h"

using namespace kuzu::planner;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace graph {

void GraphEntryTableInfo::setPredicate(std::shared_ptr<Expression> predicate_) {
    KU_ASSERT(predicate == nullptr);
    predicate = std::move(predicate_);
}

GraphEntry::GraphEntry(std::vector<TableCatalogEntry*> nodeEntries,
    std::vector<TableCatalogEntry*> relEntries) {
    for (auto& entry : nodeEntries) {
        nodeInfos.emplace_back(entry);
    }
    for (auto& entry : relEntries) {
        relInfos.emplace_back(entry);
    }
}

std::vector<table_id_t> GraphEntry::getNodeTableIDs() const {
    std::vector<table_id_t> result;
    for (auto& info : nodeInfos) {
        result.push_back(info.entry->getTableID());
    }
    return result;
}

std::vector<table_id_t> GraphEntry::getRelTableIDs() const {
    std::vector<table_id_t> result;
    for (auto& info : relInfos) {
        result.push_back(info.entry->getTableID());
    }
    return result;
}

std::vector<TableCatalogEntry*> GraphEntry::getNodeEntries() const {
    std::vector<TableCatalogEntry*> result;
    for (auto& info : nodeInfos) {
        result.push_back(info.entry);
    }
    return result;
}

std::vector<TableCatalogEntry*> GraphEntry::getRelEntries() const {
    std::vector<TableCatalogEntry*> result;
    for (auto& info : relInfos) {
        result.push_back(info.entry);
    }
    return result;
}

const GraphEntryTableInfo& GraphEntry::getRelInfo(table_id_t tableID) const {
    for (auto& info : relInfos) {
        if (info.entry->getTableID() == tableID) {
            return info;
        }
    }
    // LCOV_EXCL_START
    throw RuntimeException(stringFormat("Cannot find rel table with id {}", tableID));
    // LCOV_EXCL_STOP
}

void GraphEntry::setRelPredicate(std::shared_ptr<Expression> predicate) {
    for (auto& info : relInfos) {
        info.setPredicate(predicate);
    }
}

} // namespace graph
} // namespace kuzu
