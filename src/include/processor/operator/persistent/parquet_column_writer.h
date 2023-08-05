#pragma once

#include "processor/operator/persistent/csv_parquet_writer.h"
#include <parquet/api/writer.h>

namespace kuzu {
namespace processor {

// Additional computed information for each column
// that will be used to write values
// Each element in the vector is related to a column
// TODO (Rui): as other information is no longer needed, we can remove this struct
struct ParquetColumnDescriptor {
    int maxDefinitionLevel = 1;
};

// Kuzu column represents a single column in Kuzu, which may contains multiple Parquet columns
// writeColumn will be called for each Kuzu column.
class ParquetColumnWriter {
public:
    ParquetColumnWriter(std::unordered_map<int, ParquetColumnDescriptor> columnDescriptors)
        : columnDescriptors(columnDescriptors){};

    void writeColumn(
        int kuzuColumn, common::ValueVector* vector, parquet::RowGroupWriter* rowWriter);

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

    std::unordered_map<int, ParquetColumnDescriptor> columnDescriptors;

    // Properties for nested lists and structs
    bool isListStarting;

    int currentKuzuColumn;

    // define the writers
    parquet::Int64Writer* int64Writer;
    parquet::ByteArrayWriter* byteArrayWriter;
    parquet::FixedLenByteArrayWriter* fixedLenByteArrayWriter;
    parquet::BoolWriter* boolWriter;
    parquet::RowGroupWriter* rowWriter;
    parquet::Int32Writer* int32Writer;
    parquet::DoubleWriter* doubleWriter;
    parquet::FloatWriter* floatWriter;
};

} // namespace processor
} // namespace kuzu
