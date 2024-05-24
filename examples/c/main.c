#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "kuzu.h"

int main() {
    kuzu_database db;
    kuzu_connection conn;
    kuzu_database_init("" /* fill db path */, kuzu_default_system_config(), &db);
    kuzu_connection_init(&db, &conn);

    // Create schema.
    kuzu_query_result result;
    kuzu_connection_query(
        &conn, "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));", &result);
    kuzu_query_result_destroy(&result);
    // Create nodes.
    kuzu_connection_query(&conn, "CREATE (:Person {name: 'Alice', age: 25});", &result);
    kuzu_query_result_destroy(&result);
    kuzu_connection_query(&conn, "CREATE (:Person {name: 'Bob', age: 30});", &result);
    kuzu_query_result_destroy(&result);

    // Execute a simple query.
    kuzu_connection_query(&conn, "MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;", &result);

    // Fetch each value.
    kuzu_flat_tuple tuple;
    kuzu_value value;
    while (kuzu_query_result_has_next(&result)) {
        kuzu_query_result_get_next(&result, &tuple);

        kuzu_flat_tuple_get_value(&tuple, 0, &value);
        char* name;
        kuzu_value_get_string(&value, &name);

        kuzu_flat_tuple_get_value(&tuple, 1, &value);
        int64_t age;
        kuzu_value_get_int64(&value, &age);

        printf("name: %s, age: %" PRIi64 " \n", name, age);
        kuzu_destroy_string(name);
    }
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&tuple);

    // Print query result.
    char* result_string = kuzu_query_result_to_string(&result);
    printf("%s", result_string);
    kuzu_destroy_string(result_string);

    kuzu_query_result_destroy(&result);
    kuzu_connection_destroy(&conn);
    kuzu_database_destroy(&db);
    return 0;
}
