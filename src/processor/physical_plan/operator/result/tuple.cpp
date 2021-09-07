#include "src/processor/include/physical_plan/operator/result/tuple.h"

#include <iomanip>
#include <iostream>
#include <string>

namespace graphflow {
namespace processor {

string Tuple::toString(const vector<uint32_t>& colsWidth, const string& delimiter) {
    ostringstream result;
    for (auto i = 0ul; i < values.size(); i++) {
        string value = nullMask[i] ? "" : values[i]->toString();
        if (colsWidth[i] != 0) {
            value = " " + value + " ";
        }
        result << left << setw((int)colsWidth[i]) << setfill(' ') << value;
        if (i != values.size() - 1) {
            result << delimiter;
        }
    }
    return result.str();
}

} // namespace processor
} // namespace graphflow
