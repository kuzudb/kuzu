#include "c_api/kuzu.h"
#include "graph_test/api_graph_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class CApiRdfVariantTest : public APIRdfGraphTest {
public:
    kuzu_database _database;
    kuzu_connection connection;

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
        kuzu_database_init(databasePathCStr, systemConfig, &_database);
        kuzu_connection_init(&_database, &connection);
    }

    kuzu_database* getDatabase() { return &_database; }

    kuzu_connection* getConnection() { return &connection; }

    void TearDown() override {
        kuzu_connection_destroy(&connection);
        kuzu_database_destroy(&_database);
        APIRdfGraphTest::TearDown();
    }
};

TEST_F(CApiRdfVariantTest, GetValue) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:T_l) RETURN a.val ORDER BY a.id;", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_EQ(kuzu_query_result_get_error_message(&result), nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 16);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    auto i = 0u;
    kuzu_flat_tuple flatTuple;
    kuzu_value* badValue = kuzu_value_create_string("abcdefg");
    while (kuzu_query_result_has_next(&result)) {
        state = kuzu_query_result_get_next(&result, &flatTuple);
        ASSERT_EQ(state, KuzuSuccess);
        kuzu_value value;
        ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
        switch (i) {
        case 0: {
            kuzu_data_type_id type;
            int64_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_INT64);
            ASSERT_EQ(kuzu_rdf_variant_get_int64(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 12);

            ASSERT_EQ(kuzu_rdf_variant_get_int64(badValue, &result), KuzuError);
            break;
        }
        case 1: {
            kuzu_data_type_id type;
            int32_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_INT32);
            ASSERT_EQ(kuzu_rdf_variant_get_int32(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 43);

            ASSERT_EQ(kuzu_rdf_variant_get_int32(badValue, &result), KuzuError);
            break;
        }
        case 2: {
            kuzu_data_type_id type;
            int16_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_INT16);
            ASSERT_EQ(kuzu_rdf_variant_get_int16(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 33);

            ASSERT_EQ(kuzu_rdf_variant_get_int16(badValue, &result), KuzuError);
            break;
        }
        case 3: {
            kuzu_data_type_id type;
            int8_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_INT8);
            ASSERT_EQ(kuzu_rdf_variant_get_int8(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 2);

            ASSERT_EQ(kuzu_rdf_variant_get_int8(badValue, &result), KuzuError);
            break;
        }
        case 4: {
            kuzu_data_type_id type;
            uint64_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_UINT64);
            ASSERT_EQ(kuzu_rdf_variant_get_uint64(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 90);

            ASSERT_EQ(kuzu_rdf_variant_get_uint64(badValue, &result), KuzuError);
            break;
        }
        case 5: {
            kuzu_data_type_id type;
            uint32_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_UINT32);
            ASSERT_EQ(kuzu_rdf_variant_get_uint32(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 77);

            ASSERT_EQ(kuzu_rdf_variant_get_uint32(badValue, &result), KuzuError);
            break;
        }
        case 6: {
            kuzu_data_type_id type;
            uint16_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_UINT16);
            ASSERT_EQ(kuzu_rdf_variant_get_uint16(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 12);

            ASSERT_EQ(kuzu_rdf_variant_get_uint16(badValue, &result), KuzuError);
            break;
        }
        case 7: {
            kuzu_data_type_id type;
            uint8_t result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_UINT8);
            ASSERT_EQ(kuzu_rdf_variant_get_uint8(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, 1);

            ASSERT_EQ(kuzu_rdf_variant_get_uint8(badValue, &result), KuzuError);
            break;
        }
        case 8: {
            kuzu_data_type_id type;
            double result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_DOUBLE);
            ASSERT_EQ(kuzu_rdf_variant_get_double(&value, &result), KuzuSuccess);
            ASSERT_DOUBLE_EQ(result, 4.4);

            ASSERT_EQ(kuzu_rdf_variant_get_double(badValue, &result), KuzuError);
            break;
        }
        case 9: {
            kuzu_data_type_id type;
            float result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_FLOAT);
            ASSERT_EQ(kuzu_rdf_variant_get_float(&value, &result), KuzuSuccess);
            ASSERT_FLOAT_EQ(result, 1.2);

            ASSERT_EQ(kuzu_rdf_variant_get_float(badValue, &result), KuzuError);
            break;
        }
        case 10: {
            kuzu_data_type_id type;
            bool result;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_BOOL);
            ASSERT_EQ(kuzu_rdf_variant_get_bool(&value, &result), KuzuSuccess);
            ASSERT_EQ(result, true);

            ASSERT_EQ(kuzu_rdf_variant_get_bool(badValue, &result), KuzuError);
            break;
        }
        case 11: {
            kuzu_data_type_id type;
            char* str;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_STRING);
            ASSERT_EQ(kuzu_rdf_variant_get_string(&value, &str), KuzuSuccess);
            ASSERT_STREQ(str, "hhh");
            kuzu_destroy_string(str);

            ASSERT_EQ(kuzu_rdf_variant_get_string(badValue, &str), KuzuError);
            break;
        }
        case 12: {
            kuzu_data_type_id type;
            kuzu_date_t date;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_DATE);
            ASSERT_EQ(kuzu_rdf_variant_get_date(&value, &date), KuzuSuccess);
            ASSERT_EQ(date.days, 19723);

            ASSERT_EQ(kuzu_rdf_variant_get_date(badValue, &date), KuzuError);
            break;
        }
        case 13: {
            kuzu_data_type_id type;
            kuzu_timestamp_t time;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_TIMESTAMP);
            ASSERT_EQ(kuzu_rdf_variant_get_timestamp(&value, &time), KuzuSuccess);
            ASSERT_EQ(time.value, 1704108330000000);

            ASSERT_EQ(kuzu_rdf_variant_get_timestamp(badValue, &time), KuzuError);
            break;
        }
        case 14: {
            kuzu_data_type_id type;
            kuzu_interval_t interval;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_INTERVAL);
            ASSERT_EQ(kuzu_rdf_variant_get_interval(&value, &interval), KuzuSuccess);
            ASSERT_EQ(interval.days, 2);
            ASSERT_EQ(interval.months, 0);
            ASSERT_EQ(interval.micros, 0);

            ASSERT_EQ(kuzu_rdf_variant_get_interval(badValue, &interval), KuzuError);
            break;
        }
        case 15: {
            kuzu_data_type_id type;
            uint8_t* blob;
            ASSERT_EQ(kuzu_rdf_variant_get_type(&value, &type), KuzuSuccess);
            ASSERT_EQ(type, kuzu_data_type_id::KUZU_BLOB);
            ASSERT_EQ(kuzu_rdf_variant_get_blob(&value, &blob), KuzuSuccess);
            ASSERT_EQ(strlen((char*)blob), 1);
            ASSERT_EQ(blob[0], 0xB2);
            kuzu_destroy_blob(blob);

            ASSERT_EQ(kuzu_rdf_variant_get_blob(badValue, &blob), KuzuError);
            break;
        }

        default:
            KU_UNREACHABLE;
        }
        ASSERT_NE(value._value, nullptr);
        kuzu_value_destroy(&value);
        ++i;
    }
    kuzu_flat_tuple_destroy(&flatTuple);
    ASSERT_EQ(i, 16);
    kuzu_query_result_destroy(&result);

    kuzu_data_type_id type;
    ASSERT_EQ(kuzu_rdf_variant_get_type(badValue, &type), KuzuError);
    kuzu_value_destroy(badValue);
}
