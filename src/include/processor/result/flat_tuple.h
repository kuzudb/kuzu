#pragma once

#include "common/api.h"
#include "common/types/value.h"

namespace kuzu {
namespace processor {

/**
 * @brief Stores a vector of Values.
 */
class FlatTuple {
public:
    void addValue(std::unique_ptr<common::Value> value);

    /**
     * @return number of values in the FlatTuple.
     */
    KUZU_API uint32_t len() const;

    /**
     * @param idx value index to get.
     * @return the value stored at idx.
     */
    KUZU_API common::Value* getValue(uint32_t idx) const;

    std::string toString();

    /**
     * @param colsWidth The length of each column
     * @param delimiter The delimiter to separate each value.
     * @param maxWidth The maximum length of each column. Only the first maxWidth number of
     * characters of each column will be displayed.
     * @return all values in string format.
     */
    KUZU_API std::string toString(const std::vector<uint32_t>& colsWidth,
        const std::string& delimiter = "|", uint32_t maxWidth = -1);

private:
    std::vector<std::unique_ptr<common::Value>> values;
};

} // namespace processor
} // namespace kuzu
