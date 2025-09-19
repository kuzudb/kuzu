#include "test_runner/fsm_leak_checker.h"

#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/string_format.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "processor/result/flat_tuple.h"
#include "storage/checkpointer.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_context.h"

namespace kuzu::testing {

namespace {
std::uint64_t getDbSizeInPagesImpl(main::Connection* conn) {
    return storage::StorageManager::Get(*conn->getClientContext())->getDataFH()->getNumPages();
}

std::uint64_t getNumFreePagesImpl(main::Connection* conn) {
    auto& pm = *storage::PageManager::Get(*conn->getClientContext());
    auto numFreeEntries = pm.getNumFreeEntries();
    auto entries = pm.getFreeEntries(0, numFreeEntries);
    return std::accumulate(entries.begin(), entries.end(), 0ULL,
        [](std::uint64_t a, const auto& b) { return a + b.numPages; });
}
} // namespace

void FSMLeakChecker::checkForLeakedPages(main::Connection* conn) {
    // Skip FSM check during active transactions
    if (transaction::TransactionContext::Get(*conn->getClientContext())->hasActiveTransaction()) {
        return;
    }

    ASSERT_TRUE(conn->query("checkpoint")->isSuccess());

    // Collect all indexes - only query indexes for user tables to avoid failures on internal tables
    std::vector<std::tuple<std::string, std::string, std::string>> indexes;
    // (table name, index name, index type)

    {
        auto result = conn->query("CALL SHOW_INDEXES() RETURN *;");
        ASSERT_TRUE(result->isSuccess());

        while (result->hasNext()) {
            auto row = result->getNext();
            std::string tableName = row->getValue(0)->toString();
            std::string indexName = row->getValue(1)->toString();
            std::string indexType = row->getValue(2)->toString();

            indexes.emplace_back(tableName, indexName, indexType);
        }
    }

    // Drop all collected indexes
    for (auto& [tableName, indexName, indexType] : indexes) {
        std::string dropQuery;
        if (indexType == "FTS") {
            dropQuery =
                common::stringFormat("CALL DROP_FTS_INDEX('{}', '{}');", tableName, indexName);
        } else if (indexType == "HNSW") {
            dropQuery =
                common::stringFormat("CALL DROP_VECTOR_INDEX('{}', '{}');", tableName, indexName);
        } else {
            EXPECT_TRUE(false) << "Unknown index type: " << indexType << " (table=" << tableName
                               << ", index=" << indexName << ")" << std::endl;
            continue;
        }

        auto result = conn->query(dropQuery);
        ASSERT_TRUE(result->isSuccess())
            << "Failed to drop index " << indexName << " on " << tableName << std::endl;
    }

    // Enable internal catalog to see all tables including hidden ones
    ASSERT_TRUE(conn->query("call enable_internal_catalog=true")->isSuccess());

    // Collect all table names
    std::vector<std::pair<std::string, std::string>> tableNames;
    // (name, type)
    auto tableNamesResult = conn->query("call show_tables() return name, type");
    while (tableNamesResult->hasNext()) {
        auto next = tableNamesResult->getNext();
        tableNames.emplace_back(next->getValue(0)->toString(), next->getValue(1)->toString());
    }

    // Drop rel tables first
    for (const auto& [name, type] : tableNames) {
        if (type == common::TableTypeUtils::toString(common::TableType::REL)) {
            ASSERT_TRUE(conn->query(common::stringFormat("drop table `{}`", name))->isSuccess());
        }
    }
    // Then non-rel
    for (const auto& [name, type] : tableNames) {
        if (type != common::TableTypeUtils::toString(common::TableType::REL)) {
            ASSERT_TRUE(conn->query(common::stringFormat("drop table `{}`", name))->isSuccess());
        }
    }

    ASSERT_TRUE(conn->query("call enable_internal_catalog=false")->isSuccess());

    ASSERT_TRUE(conn->query("checkpoint")->isSuccess());

    const auto numTotalPages = getDbSizeInPagesImpl(conn);
    const auto numUsedPages = numTotalPages - getNumFreePagesImpl(conn);

    storage::Checkpointer checkpointer{*conn->getClientContext()};
    auto databaseHeader = storage::StorageManager::Get(*conn->getClientContext())
                              ->getOrInitDatabaseHeader(*conn->getClientContext());

    ASSERT_EQ(1 + databaseHeader->catalogPageRange.numPages +
                  databaseHeader->metadataPageRange.numPages,
        numUsedPages);
}

} // namespace kuzu::testing
