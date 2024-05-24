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

kuzu_state kuzu_connection_init(kuzu_database* database, kuzu_connection* out_connection) {
    if (database == nullptr || database->_database == nullptr) {
        out_connection->_connection = nullptr;
        return KuzuError;
    }
    try {
        out_connection->_connection = new Connection(static_cast<Database*>(database->_database));
    } catch (Exception& e) {
        out_connection->_connection = nullptr;
        return KuzuError;
    }
    return KuzuSuccess;
}

void kuzu_connection_destroy(kuzu_connection* connection) {
    if (connection == nullptr) {
        return;
    }
    if (connection->_connection != nullptr) {
        delete static_cast<Connection*>(connection->_connection);
    }
}

kuzu_state kuzu_connection_set_max_num_thread_for_exec(kuzu_connection* connection,
    uint64_t num_threads) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return KuzuError;
    }
    try {
        static_cast<Connection*>(connection->_connection)->setMaxNumThreadForExec(num_threads);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_connection_get_max_num_thread_for_exec(kuzu_connection* connection,
    uint64_t* out_result) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Connection*>(connection->_connection)->getMaxNumThreadForExec();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_connection_query(kuzu_connection* connection, const char* query,
    kuzu_query_result* out_query_result) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return KuzuError;
    }
    try {
        auto query_result =
            static_cast<Connection*>(connection->_connection)->query(query).release();
        if (query_result == nullptr) {
            return KuzuError;
        }
        out_query_result->_query_result = query_result;
        out_query_result->_is_owned_by_cpp = false;
        if (!query_result->isSuccess()) {
            return KuzuError;
        }
        return KuzuSuccess;
    } catch (Exception& e) {
        return KuzuError;
    }
}

kuzu_state kuzu_connection_prepare(kuzu_connection* connection, const char* query,
    kuzu_prepared_statement* out_prepared_statement) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return KuzuError;
    }
    try {
        auto prepared_statement =
            static_cast<Connection*>(connection->_connection)->prepare(query).release();
        if (prepared_statement == nullptr) {
            return KuzuError;
        }
        out_prepared_statement->_prepared_statement = prepared_statement;
        out_prepared_statement->_bound_values =
            new std::unordered_map<std::string, std::unique_ptr<Value>>;
        return KuzuSuccess;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_connection_execute(kuzu_connection* connection,
    kuzu_prepared_statement* prepared_statement, kuzu_query_result* out_query_result) {
    if (connection == nullptr || connection->_connection == nullptr ||
        prepared_statement == nullptr || prepared_statement->_prepared_statement == nullptr ||
        prepared_statement->_bound_values == nullptr) {
        return KuzuError;
    }
    try {
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
            return KuzuError;
        }
        out_query_result->_query_result = query_result;
        out_query_result->_is_owned_by_cpp = false;
        if (!query_result->isSuccess()) {
            return KuzuError;
        }
        return KuzuSuccess;
    } catch (Exception& e) {
        return KuzuError;
    }
}
void kuzu_connection_interrupt(kuzu_connection* connection) {
    static_cast<Connection*>(connection->_connection)->interrupt();
}

kuzu_state kuzu_connection_set_query_timeout(kuzu_connection* connection, uint64_t timeout_in_ms) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return KuzuError;
    }
    try {
        static_cast<Connection*>(connection->_connection)->setQueryTimeOut(timeout_in_ms);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}
