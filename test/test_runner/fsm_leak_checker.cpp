#include "test_runner/fsm_leak_checker.h"

#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "common/string_format.h"
#include "common/types/types.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "processor/result/flat_tuple.h"
#include "storage/checkpointer.h"
#include "storage/storage_manager.h"

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
    ASSERT_TRUE(conn->query("checkpoint")->isSuccess());

    std::vector<std::pair<std::string, std::string>> tableNames;
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
