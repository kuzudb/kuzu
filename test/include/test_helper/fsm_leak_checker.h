#pragma once

#include <numeric>
#include <vector>

#include "common/string_format.h"
#include "common/types/types.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "storage/checkpointer.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace testing {

static decltype(auto) getDbSizeInPages(main::Connection* conn) {
    return storage::StorageManager::Get(*conn->getClientContext())->getDataFH()->getNumPages();
}

static decltype(auto) getNumFreePages(main::Connection* conn) {
    auto& pageManager = *storage::PageManager::Get(*conn->getClientContext());
    auto numFreeEntries = pageManager.getNumFreeEntries();
    auto entries = pageManager.getFreeEntries(0, numFreeEntries);
    return std::accumulate(entries.begin(), entries.end(), 0ull,
        [](auto a, auto b) { return a + b.numPages; });
}

struct FSMLeakChecker {
    static void checkForLeakedPages(main::Connection* conn) {
        conn->query("checkpoint");

        std::vector<std::pair<std::string, std::string>> tableNames;
        auto tableNamesResult = conn->query("call show_tables() return name, type");
        while (tableNamesResult->hasNext()) {
            auto nextResult = tableNamesResult->getNext();
            tableNames.emplace_back(nextResult->getValue(0)->toString(),
                nextResult->getValue(1)->toString());
        }

        // Drop rel tables first
        for (const auto& [tableName, tableType] : tableNames) {
            if (tableType == common::TableTypeUtils::toString(common::TableType::REL)) {
                ASSERT_TRUE(
                    conn->query(common::stringFormat("drop table {}", tableName))->isSuccess());
            }
        }
        for (const auto& [tableName, tableType] : tableNames) {
            if (tableType != common::TableTypeUtils::toString(common::TableType::REL)) {
                ASSERT_TRUE(
                    conn->query(common::stringFormat("drop table {}", tableName))->isSuccess());
            }
        }
        conn->query("checkpoint");

        const auto numTotalPages = getDbSizeInPages(conn);
        const auto numUsedPages = numTotalPages - getNumFreePages(conn);

        storage::Checkpointer checkpointer{*conn->getClientContext()};
        auto databaseHeader = storage::StorageManager::Get(*conn->getClientContext())
                                  ->getOrInitDatabaseHeader(*conn->getClientContext());
        ASSERT_EQ(1 + databaseHeader->catalogPageRange.numPages +
                      databaseHeader->metadataPageRange.numPages,
            numUsedPages);
    }
};

} // namespace testing
} // namespace kuzu