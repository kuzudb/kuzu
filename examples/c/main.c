#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "c_api/kuzu.h"

int main() {
    kuzu_database* db = kuzu_database_init("" /* fill db path */, kuzu_default_system_config());
    kuzu_connection* conn = kuzu_connection_init(db);

    kuzu_query_result* result;
    // Create schema.
    result = kuzu_connection_query(
        conn, "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
    kuzu_query_result_destroy(result);
    // Create nodes.
    result = kuzu_connection_query(conn, "CREATE (:Person {name: 'Alice', age: 25});");
    kuzu_query_result_destroy(result);
    result = kuzu_connection_query(conn, "CREATE (:Person {name: 'Bob', age: 30});");
    kuzu_query_result_destroy(result);

    // Execute a simple query.
    result = kuzu_connection_query(conn, "MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");

    // Fetch each value.
    while (kuzu_query_result_has_next(result)) {
        kuzu_flat_tuple* tuple = kuzu_query_result_get_next(result);

        kuzu_value* value = kuzu_flat_tuple_get_value(tuple, 0);
        char* name = kuzu_value_get_string(value);
        kuzu_value_destroy(value);

        value = kuzu_flat_tuple_get_value(tuple, 1);
        int64_t age = kuzu_value_get_int64(value);
        kuzu_value_destroy(value);

        printf("name: %s, age: %" PRIi64 " \n", name, age);
        free(name);
        kuzu_flat_tuple_destroy(tuple);
    }

    // Print query result.
    char* result_string = kuzu_query_result_to_string(result);
    printf("%s", kuzu_query_result_to_string(result));
    free(result_string);

    kuzu_query_result_destroy(result);
    kuzu_connection_destroy(conn);
    kuzu_database_destroy(db);
    return 0;
}
