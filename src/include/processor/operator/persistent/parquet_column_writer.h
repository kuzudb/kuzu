#pragma once

#include "common/types/date_t.h"
#include "common/types/timestamp_t.h"
#include "processor/operator/persistent/file_writer.h"
#include <parquet/api/writer.h>

namespace kuzu {
namespace processor {

// Kuzu column represents a single column in Kuzu, which may contains multiple Parquet columns
// writeColumn will be called for each Kuzu column.
class ParquetColumnWriter {
public:
    ParquetColumnWriter(int totalColumns, std::vector<int> maxDefinitionLevels)
        : maxDefinitionLevels(maxDefinitionLevels), totalColumns(totalColumns){};

    void writeColumn(
        int column, common::ValueVector* vector, parquet::RowGroupWriter* rowGroupWriter);

    int64_t estimatedRowBytes;

private:
    void nextParquetColumn(common::LogicalTypeID logicalTypeID);
    void writePrimitiveValue(common::LogicalTypeID logicalTypeID, uint8_t* value,
        int16_t definitionLevel = 0, int16_t repetitionLevel = 0);

    // ParquetBatch contains the information needed by low-level arrow API writeBatch:
    // WriteBatch(int64_t num_values, const int16_t* def_levels,
    //               const int16_t* rep_levels, const T* values
    struct ParquetColumn {
        common::LogicalTypeID logicalTypeID;
        std::vector<int16_t> repetitionLevels;
        std::vector<int16_t> definitionLevels;
        std::vector<uint8_t*> values;
    };

    void addToParquetColumns(uint8_t* value, common::ValueVector* vector,
        std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx = 0,
        int parentElementIdx = 0, int depth = 0, std::string parentStructFieldName = "");

    void extractList(const common::list_entry_t& list, const common::ValueVector* vector,
        std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx = 0,
        int parentElementIdx = 0, int depth = 0, std::string parentStructFieldName = "");

    void extractStruct(const common::struct_entry_t& val, const common::ValueVector* vector,
        std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx = 0,
        int parentElementIdx = 0, int depth = 0, std::string parentStructFieldName = "");

    void extractNested(uint8_t* value, const common::ValueVector* vector,
        std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx = 0,
        int parentElementIdx = 0, int depth = 0, std::string parentStructFieldName = "");

    inline void initNewColumn() { isListStarting = true; }

    void writeLittleEndianUint32(uint8_t* buffer, uint32_t value);

    // Extract dremel encoding levels
    int getRepetitionLevel(int currentElementIdx, int parentElementIdx, int depth);

    std::vector<int> maxDefinitionLevels;

    // Properties for nested lists and structs
    bool isListStarting;

    int currentColumn = 0;
    int currentParquetColumn = 0;
    int totalColumns = 0;

    // define the writers
    parquet::RowGroupWriter* rowGroupWriter;
    parquet::ColumnWriter* columnWriter;
};

} // namespace processor
} // namespace kuzu
