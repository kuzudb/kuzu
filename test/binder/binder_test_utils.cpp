#include "test/binder/binder_test_utils.h"

bool BinderTestUtils::equals(
    const BoundLoadCSVStatement& left, const BoundLoadCSVStatement& right) {
    auto result = left.filePath == right.filePath && left.tokenSeparator == right.tokenSeparator &&
                  left.lineVariableName == right.lineVariableName &&
                  left.headerInfo.size() == right.headerInfo.size();
    if (!result) {
        return false;
    }
    for (auto i = 0u; i < left.headerInfo.size(); ++i) {
        if (left.headerInfo[i].first != right.headerInfo[i].first ||
            left.headerInfo[i].second != right.headerInfo[i].second) {
            return false;
        }
    }
    return true;
}
