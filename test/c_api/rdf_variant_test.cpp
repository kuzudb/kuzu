#include "c_api/kuzu.h"
#include "graph_test/api_graph_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class CApiRdfVariantTest : public APIRdfGraphTest {
public:
    kuzu_database* _database;
    kuzu_connection* connection;

    void SetUp() override {
        APIRdfGraphTest::SetUp();
        APIRdfGraphTest::createDBAndConn();
        APIRdfGraphTest::initGraph();
        // In C API tests, we don't use the database and connection created by DBTest because
        // they are not C++ objects.
        conn.reset();
        database.reset();
        auto databasePathCStr = databasePath.c_str();
        auto systemConfig = kuzu_default_system_config();
        systemConfig.buffer_pool_size = 512 * 1024 * 1024;
        _database = kuzu_database_init(databasePathCStr, systemConfig);
        connection = kuzu_connection_init(_database);
    }

    kuzu_database* getDatabase() const { return _database; }

    kuzu_connection* getConnection() const { return connection; }

    void TearDown() override {
        kuzu_connection_destroy(connection);
        kuzu_database_destroy(_database);
        APIRdfGraphTest::TearDown();
    }
};

TEST_F(CApiRdfVariantTest, GetValue) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection, "MATCH (a:T_l) RETURN a.val ORDER BY a.id;");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_EQ(kuzu_query_result_get_error_message(result), nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 16);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    auto i = 0u;
    while (kuzu_query_result_has_next(result)) {
        auto flatTuple = kuzu_query_result_get_next(result);
        ASSERT_NE(flatTuple, nullptr);
        auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
        switch (i) {
        case 0: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_INT64);
            ASSERT_EQ(kuzu_rdf_variant_get_int64(value), 12);
            break;
        }
        case 1: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_INT32);
            ASSERT_EQ(kuzu_rdf_variant_get_int32(value), 43);
            break;
        }
        case 2: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_INT16);
            ASSERT_EQ(kuzu_rdf_variant_get_int16(value), 33);
            break;
        }
        case 3: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_INT8);
            ASSERT_EQ(kuzu_rdf_variant_get_int8(value), 2);
            break;
        }
        case 4: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_UINT64);
            ASSERT_EQ(kuzu_rdf_variant_get_uint64(value), 90);
            break;
        }
        case 5: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_UINT32);
            ASSERT_EQ(kuzu_rdf_variant_get_uint32(value), 77);
            break;
        }
        case 6: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_UINT16);
            ASSERT_EQ(kuzu_rdf_variant_get_uint16(value), 12);
            break;
        }
        case 7: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_UINT8);
            ASSERT_EQ(kuzu_rdf_variant_get_uint8(value), 1);
            break;
        }
        case 8: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_DOUBLE);
            ASSERT_DOUBLE_EQ(kuzu_rdf_variant_get_double(value), 4.4);
            break;
        }

        case 9: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_FLOAT);
            ASSERT_FLOAT_EQ(kuzu_rdf_variant_get_float(value), 1.2);
            break;
        }
        case 10: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_BOOL);
            ASSERT_EQ(kuzu_rdf_variant_get_bool(value), true);
            break;
        }
        case 11: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_STRING);
            auto str = kuzu_rdf_variant_get_string(value);
            ASSERT_STREQ(str, "hhh");
            kuzu_destroy_string(str);
            break;
        }
        case 12: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_DATE);
            auto date = kuzu_rdf_variant_get_date(value);
            ASSERT_EQ(date.days, 19723);
            break;
        }
        case 13: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_TIMESTAMP);
            auto time = kuzu_rdf_variant_get_timestamp(value);
            ASSERT_EQ(time.value, 1704108330000000);
            break;
        }
        case 14: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_INTERVAL);
            auto interval = kuzu_rdf_variant_get_interval(value);
            ASSERT_EQ(interval.days, 2);
            ASSERT_EQ(interval.months, 0);
            ASSERT_EQ(interval.micros, 0);
            break;
        }

        case 15: {
            ASSERT_EQ(kuzu_rdf_variant_get_type(value), kuzu_data_type_id::KUZU_BLOB);
            auto blob = kuzu_rdf_variant_get_blob(value);
            ASSERT_EQ(strlen((char*)blob), 1);
            ASSERT_EQ(blob[0], 0xB2);
            kuzu_destroy_string((char*)blob);
            break;
        }

        default:
            KU_UNREACHABLE;
        }
        ASSERT_NE(value, nullptr);
        kuzu_value_destroy(value);
        kuzu_flat_tuple_destroy(flatTuple);
        ++i;
    }
    ASSERT_EQ(i, 16);
    kuzu_query_result_destroy(result);
}
