#pragma once

#include "robin_hood.h"

using namespace std;

namespace graphflow {
namespace common {

// C-like string equalTo.
struct charArrayEqualTo {
    bool operator()(const char* lhs, const char* rhs) const { return strcmp(lhs, rhs) == 0; }
};

// C-like string hasher.
struct charArrayHasher {
    size_t operator()(const char* key) const { return robin_hood::hash_bytes(key, strlen(key)); }
};

class StringUtils {

public:
    static vector<string> split(const string& input, const string& delimiter);
};

} // namespace common
} // namespace graphflow
