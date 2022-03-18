#include "src/processor/include/physical_plan/result/flat_tuple.h"

#include <iomanip>
#include <sstream>
#include <string>

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace processor {

string FlatTuple::toString(const vector<uint32_t>& colsWidth, const string& delimiter) {
    ostringstream result;
    for (auto i = 0ul; i < values.size(); i++) {
        string value = nullMask[i] ? "" : TypeUtils::toString(*values[i]);
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
