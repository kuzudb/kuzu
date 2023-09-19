#include "catalog/table_schema.h"
#include "graph_test/graph_test.h"
#include "storage/stats/nodes_statistics_and_deleted_ids.h"
#include "storage/stats/table_statistics.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

TEST(TableStatisticsTest, CopyTableStatistics) {
    auto numTuples = 20;

    std::unordered_map<property_id_t, std::unique_ptr<PropertyStatistics>> propertyStatistics;
    propertyStatistics[0] = std::make_unique<PropertyStatistics>(true);
    propertyStatistics[1] = std::make_unique<PropertyStatistics>(false);
    TableStatistics tableStats(numTuples, std::move(propertyStatistics));
    TableStatistics copy(tableStats);

    ASSERT_EQ(copy.getNumTuples(), numTuples);
    ASSERT_EQ(copy.getPropertyStatistics(0).mayHaveNull(), true);
    ASSERT_EQ(copy.getPropertyStatistics(1).mayHaveNull(), false);
}

TEST(TableStatisticsTest, PropertyStatisticsSetHasNull) {
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);

    NodesStatisticsAndDeletedIDs tablesStatistics;
    std::vector<std::unique_ptr<Property>> properties;
    auto propertyID = 0;
    auto tableID = 0;
    properties.push_back(std::make_unique<Property>(
        "testproperty", std::make_unique<LogicalType>(LogicalTypeID::BOOL), tableID, propertyID));
    auto schema = NodeTableSchema("TestTable", tableID, propertyID, std::move(properties));
    tablesStatistics.addTableStatistic(&schema);

    auto propertyStatistics = tablesStatistics.getPropertyStatisticsForTable(
        DUMMY_WRITE_TRANSACTION, tableID, propertyID);

    ASSERT_FALSE(propertyStatistics.mayHaveNull());
    propertyStatistics.setHasNull();
    ASSERT_TRUE(propertyStatistics.mayHaveNull());
}

TEST(TableStatisticsTest, RWPropertyStatsSetHasNull) {
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);

    NodesStatisticsAndDeletedIDs tablesStatistics;
    std::vector<std::unique_ptr<Property>> properties;
    auto propertyID = 0;
    auto tableID = 0;
    properties.push_back(std::make_unique<Property>(
        "testproperty", std::make_unique<LogicalType>(LogicalTypeID::BOOL), tableID, propertyID));
    auto schema = NodeTableSchema("TestTable", tableID, propertyID, std::move(properties));
    tablesStatistics.addTableStatistic(&schema);
    RWPropertyStats stats(&tablesStatistics, tableID, propertyID);

    ASSERT_FALSE(stats.mayHaveNull(DUMMY_WRITE_TRANSACTION));
    stats.setHasNull(DUMMY_WRITE_TRANSACTION);
    ASSERT_TRUE(stats.mayHaveNull(DUMMY_WRITE_TRANSACTION));
}
