#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("dsada" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    connection->query("CREATE NODE TABLE Entity (\n"
                      "        node_id STRING,\n"
                      "        role STRING,\n"
                      "        name STRING,\n"
                      "        original_name STRING,\n"
                      "        former_name STRING,\n"
                      "        jurisdiction STRING,\n"
                      "        jurisdiction_description STRING,\n"
                      "        company_type STRING,\n"
                      "        address STRING,\n"
                      "        internal_id STRING,\n"
                      "        incorporation_date STRING,\n"
                      "        inactivation_date STRING,\n"
                      "        struck_off_date STRING,\n"
                      "        dorm_date STRING,\n"
                      "        status STRING,\n"
                      "        service_provider STRING,\n"
                      "        ibcRUC STRING,\n"
                      "        country_codes STRING,\n"
                      "        countries STRING,\n"
                      "        sourceID STRING,\n"
                      "        valid_until STRING,\n"
                      "        note STRING,\n"
                      "        vague BOOLEAN,\n"
                      "        PRIMARY KEY (node_id)\n"
                      "    )");
}
