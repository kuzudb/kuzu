#pragma once

#include "src/main/include/connection.h"

using namespace std;

namespace graphflow {
namespace main {

class JOConnection : public Connection {

public:
    explicit JOConnection(Database* database) : Connection{database} {}

    unique_ptr<QueryResult> query(const string& query, const string& encodedJoin);
};

} // namespace main
} // namespace graphflow
