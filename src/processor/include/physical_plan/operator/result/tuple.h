#pragma once

#include "src/common/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

class Tuple {

public:
    explicit Tuple(const vector<DataType>& valueTypes) {
        values.resize(valueTypes.size());
        nullMask.resize(valueTypes.size());
        for (auto i = 0u; i < valueTypes.size(); i++) {
            values[i] = make_unique<Value>(valueTypes[i]);
            nullMask[i] = false;
        }
    }

    inline Value* getValue(uint64_t valIdx) { return values[valIdx].get(); }

    string toString(const string& delimiter = "|") {
        string result;
        for (auto i = 0u; i < values.size() - 1; i++) {
            if (!nullMask[i]) {
                result += values[i]->toString();
            }
            result += delimiter;
        }
        if (!nullMask[values.size() - 1]) {
            result += values[values.size() - 1]->toString();
        }
        return result;
    }

public:
    vector<bool> nullMask;

private:
    vector<unique_ptr<Value>> values;
};

} // namespace processor
} // namespace graphflow
