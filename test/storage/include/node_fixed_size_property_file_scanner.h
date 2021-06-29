#include <memory>

#include "src/common/include/types.h"

namespace graphflow {
namespace storage {
class NodeFixedSizePropertyFileScanner {

public:
    NodeFixedSizePropertyFileScanner(const string& directory,
        const graphflow::common::label_t& nodeLabel, const string& propertyName);

    template<typename T>
    T readProperty(int nodeOffset) {
        auto offset = nodeOffset * sizeof(T);
        return *(T*)(buffer.get() + offset);
    };

private:
    unique_ptr<char[]> buffer;
};

} // namespace storage
} // namespace graphflow