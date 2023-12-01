#include "c_api/kuzu.h"
#include "common/exception/exception.h"
#include "main/kuzu.h"

namespace kuzu {
namespace common {
class Value;
}
} // namespace kuzu

using namespace kuzu::common;
using namespace kuzu::main;

kuzu_connection* kuzu_connection_init(kuzu_database* database) {
    if (database == nullptr || database->_database == nullptr) {
        return nullptr;
    }
    auto connection = (kuzu_connection*)malloc(sizeof(kuzu_connection));
    try {
        auto* connection_ptr = new Connection(static_cast<Database*>(database->_database));
        connection->_connection = connection_ptr;
    } catch (Exception& e) {
        free(connection);
        return nullptr;
    }
    return connection;
}

void kuzu_connection_destroy(kuzu_connection* connection) {
    if (connection == nullptr) {
        return;
    }
    if (connection->_connection != nullptr) {
        delete static_cast<Connection*>(connection->_connection);
    }
    free(connection);
}

void kuzu_connection_set_max_num_thread_for_exec(
    kuzu_connection* connection, uint64_t num_threads) {
    static_cast<Connection*>(connection->_connection)->setMaxNumThreadForExec(num_threads);
}

uint64_t kuzu_connection_get_max_num_thread_for_exec(kuzu_connection* connection) {
    return static_cast<Connection*>(connection->_connection)->getMaxNumThreadForExec();
}

kuzu_query_result* kuzu_connection_query(kuzu_connection* connection, const char* query) {
    try {
        auto query_result =
            static_cast<Connection*>(connection->_connection)->query(query).release();
        if (query_result == nullptr) {
            return nullptr;
        }
        auto* c_query_result = new kuzu_query_result;
        c_query_result->_query_result = query_result;
        return c_query_result;
    } catch (Exception& e) { return nullptr; }
}

kuzu_prepared_statement* kuzu_connection_prepare(kuzu_connection* connection, const char* query) {
    auto connection_ptr = static_cast<Connection*>(connection->_connection);
    auto prepared_statement = connection_ptr->prepare(query).release();
    if (prepared_statement == nullptr) {
        return nullptr;
    }
    auto* c_prepared_statement = new kuzu_prepared_statement;
    c_prepared_statement->_prepared_statement = prepared_statement;
    c_prepared_statement->_bound_values =
        new std::unordered_map<std::string, std::unique_ptr<Value>>;
    return c_prepared_statement;
}

kuzu_query_result* kuzu_connection_execute(
    kuzu_connection* connection, kuzu_prepared_statement* prepared_statement) {
    auto prepared_statement_ptr =
        static_cast<PreparedStatement*>(prepared_statement->_prepared_statement);
    auto bound_values = static_cast<std::unordered_map<std::string, std::unique_ptr<Value>>*>(
        prepared_statement->_bound_values);

    // Must copy the parameters for safety, and so that the parameters in the prepared statement
    // stay the same.
    std::unordered_map<std::string, std::unique_ptr<Value>> copied_bound_values;
    for (auto& [name, value] : *bound_values) {
        copied_bound_values.emplace(name, value->copy());
    }

    auto query_result =
        static_cast<Connection*>(connection->_connection)
            ->executeWithParams(prepared_statement_ptr, std::move(copied_bound_values))
            .release();
    if (query_result == nullptr) {
        return nullptr;
    }
    auto* c_query_result = new kuzu_query_result;
    c_query_result->_query_result = query_result;
    return c_query_result;
}

void kuzu_connection_interrupt(kuzu_connection* connection) {
    static_cast<Connection*>(connection->_connection)->interrupt();
}

void kuzu_connection_set_query_timeout(kuzu_connection* connection, uint64_t timeout_in_ms) {
    static_cast<Connection*>(connection->_connection)->setQueryTimeOut(timeout_in_ms);
}
