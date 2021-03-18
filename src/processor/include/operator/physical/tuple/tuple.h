#pragma once

#include "src/common/include/literal.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

class Tuple {

public:
    Tuple(vector<DataType> valueTypes) {
        for (auto& valueType : valueTypes) {
            auto value = make_unique<Literal>(valueType);
            values.push_back(move(value));
        }
    }

    inline Literal* getValue(uint64_t valIdx) { return values[valIdx].get(); }

    string toString(const string& delimiter = "|") {
        string result;
        for (uint64_t i = 0; i < values.size() - 1; i++) {
            result += values[i]->toString() + delimiter;
        }
        result += values[values.size() - 1]->toString();
        return result;
    }

private:
    vector<unique_ptr<Literal>> values;
};

} // namespace processor
} // namespace graphflow
