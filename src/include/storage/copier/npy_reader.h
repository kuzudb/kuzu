#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"

namespace kuzu {
namespace storage {

class NpyReader {
public:
    explicit NpyReader(const std::string& filePath);

    ~NpyReader();

    size_t getNumElementsPerRow() const;

    void* getPointerToRow(size_t row) const;

    inline std::string getFilePath() const { return filePath; }

    inline size_t getNumRows() const { return shape[0]; }

    // Used in tests only.
    inline common::DataTypeID getType() const { return type; }
    inline std::vector<size_t> const& getShape() const { return shape; }
    inline size_t getNumDimensions() const { return shape.size(); }

    void validate(common::DataType& type_, common::offset_t numRows, const std::string& tableName);

private:
    void parseHeader();
    void parseType(std::string descr);

private:
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
