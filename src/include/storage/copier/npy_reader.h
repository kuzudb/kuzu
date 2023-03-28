#pragma once
#include <string>
#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "common/types/types.h"

namespace kuzu {
namespace storage {

class NpyReader {

public:
    explicit NpyReader(const std::string& filePath);

    ~NpyReader();

    size_t getNumElementsPerRow() const;

    void* getPointerToRow(size_t row) const;

    inline std::string getFileName() const { return filePath; }

    inline size_t getLength() const { return shape[0]; }

    inline common::DataTypeID getType() const { return type; }

    inline std::vector<size_t> const& getShape() const { return shape; }

    inline size_t getNumDimensions() const { return shape.size(); }

    inline size_t getMaxOffset() const { return fileSize - dataOffset; }

private:
    void parseHeader();

    void parseType(std::string descr);

    std::string filePath;
    int fd;
    size_t fileSize;
    void* mmapRegion;
    size_t dataOffset;
    std::vector<size_t> shape;
    common::DataTypeID type;
};

} // namespace storage
} // namespace kuzu
