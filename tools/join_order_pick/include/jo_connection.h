#pragma once

#include "main/connection.h"

using namespace std;

namespace kuzu {
namespace main {

class JOConnection : public Connection {

public:
    explicit JOConnection(Database* database) : Connection{database} {}

    unique_ptr<QueryResult> query(const string& query, const string& encodedJoin);
};

} // namespace main
} // namespace kuzu
