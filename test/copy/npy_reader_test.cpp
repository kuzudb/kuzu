#include "common/exception.h"
#include "graph_test/graph_test.h"
#include "storage/copier/npy_reader.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class NpyReaderTest : public Test {
public:
    static std::string getInputDirForOneDimensional() {
        return TestHelper::appendKuzuRootPath("dataset/npy-1d/");
    }
    static std::string getInputDirForTwoDimensional() {
        return TestHelper::appendKuzuRootPath("dataset/npy-2d/");
    }
    static std::string getInputDirForThreeDimensional() {
        return TestHelper::appendKuzuRootPath("dataset/npy-3d/");
    }
};

TEST_F(NpyReaderTest, ReadNpyOneDimensionalInt64) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForOneDimensional() + "one_dim_int64.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 1);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT64);
    ASSERT_EQ(npyReader->getNumDimensions(), 1);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    auto row = (int64_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);

    row = (int64_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 2);
    ASSERT_EQ(row[1], 3);

    row = (int64_t*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 3);
}

TEST_F(NpyReaderTest, ReadNpyTwoDimensionalInt64) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForTwoDimensional() + "two_dim_int64.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 3);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT64);
    ASSERT_EQ(npyReader->getNumDimensions(), 2);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    ASSERT_EQ(shape[1], 3);
    auto row = (int64_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);

    row = (int64_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 4);
    ASSERT_EQ(row[1], 5);
    ASSERT_EQ(row[2], 6);

    row = (int64_t*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 7);
    ASSERT_EQ(row[1], 8);
    ASSERT_EQ(row[2], 9);
}

TEST_F(NpyReaderTest, ReadNpyThreeDimensionalInt64) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForThreeDimensional() + "three_dim_int64.npy");
    ASSERT_EQ(npyReader->getNumRows(), 2);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 12);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT64);
    ASSERT_EQ(npyReader->getNumDimensions(), 3);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 2);
    ASSERT_EQ(shape[1], 3);
    ASSERT_EQ(shape[2], 4);
    auto row = (int64_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);
    ASSERT_EQ(row[3], 4);
    ASSERT_EQ(row[4], 5);
    ASSERT_EQ(row[5], 6);
    ASSERT_EQ(row[6], 7);
    ASSERT_EQ(row[7], 8);
    ASSERT_EQ(row[8], 9);
    ASSERT_EQ(row[9], 10);
    ASSERT_EQ(row[10], 11);
    ASSERT_EQ(row[11], 12);

    row = (int64_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 13);
    ASSERT_EQ(row[1], 14);
    ASSERT_EQ(row[2], 15);
    ASSERT_EQ(row[3], 16);
    ASSERT_EQ(row[4], 17);
    ASSERT_EQ(row[5], 18);
    ASSERT_EQ(row[6], 19);
    ASSERT_EQ(row[7], 20);
    ASSERT_EQ(row[8], 21);
    ASSERT_EQ(row[9], 22);
    ASSERT_EQ(row[10], 23);
    ASSERT_EQ(row[11], 24);
}

TEST_F(NpyReaderTest, ReadNpyOneDimensionalInt32) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForOneDimensional() + "one_dim_int32.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 1);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT32);
    ASSERT_EQ(npyReader->getNumDimensions(), 1);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    auto row = (int32_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);

    row = (int32_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 2);
    ASSERT_EQ(row[1], 3);

    row = (int32_t*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 3);
}

TEST_F(NpyReaderTest, ReadNpyTwoDimensionalInt32) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForTwoDimensional() + "two_dim_int32.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 3);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT32);
    ASSERT_EQ(npyReader->getNumDimensions(), 2);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    ASSERT_EQ(shape[1], 3);
    auto row = (int32_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);

    row = (int32_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 4);
    ASSERT_EQ(row[1], 5);
    ASSERT_EQ(row[2], 6);

    row = (int32_t*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 7);
    ASSERT_EQ(row[1], 8);
    ASSERT_EQ(row[2], 9);
}

TEST_F(NpyReaderTest, ReadNpyOneDimensionalInt16) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForOneDimensional() + "one_dim_int16.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 1);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT16);
    ASSERT_EQ(npyReader->getNumDimensions(), 1);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    auto row = (int16_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);

    row = (int16_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 2);
    ASSERT_EQ(row[1], 3);

    row = (int16_t*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 3);
}

TEST_F(NpyReaderTest, ReadNpyTwoDimensionalInt16) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForTwoDimensional() + "two_dim_int16.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 3);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::INT16);
    ASSERT_EQ(npyReader->getNumDimensions(), 2);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    ASSERT_EQ(shape[1], 3);
    auto row = (int16_t*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1);
    ASSERT_EQ(row[1], 2);
    ASSERT_EQ(row[2], 3);

    row = (int16_t*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 4);
    ASSERT_EQ(row[1], 5);
    ASSERT_EQ(row[2], 6);

    row = (int16_t*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 7);
    ASSERT_EQ(row[1], 8);
    ASSERT_EQ(row[2], 9);
}

TEST_F(NpyReaderTest, ReadNpyOneDimensionalDouble) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForOneDimensional() + "one_dim_double.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 1);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::DOUBLE);
    ASSERT_EQ(npyReader->getNumDimensions(), 1);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    auto row = (double*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1.0);
    ASSERT_EQ(row[1], 2.0);
    ASSERT_EQ(row[2], 3.0);

    row = (double*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 2.0);
    ASSERT_EQ(row[1], 3.0);

    row = (double*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 3.0);
}

TEST_F(NpyReaderTest, ReadNpyTwoDimensionalDouble) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForTwoDimensional() + "two_dim_double.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 3);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::DOUBLE);
    ASSERT_EQ(npyReader->getNumDimensions(), 2);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    ASSERT_EQ(shape[1], 3);
    auto row = (double*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1.0);
    ASSERT_EQ(row[1], 2.0);
    ASSERT_EQ(row[2], 3.0);

    row = (double*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 4.0);
    ASSERT_EQ(row[1], 5.0);
    ASSERT_EQ(row[2], 6.0);

    row = (double*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 7.0);
    ASSERT_EQ(row[1], 8.0);
    ASSERT_EQ(row[2], 9.0);
}

TEST_F(NpyReaderTest, ReadNpyOneDimensionalFloat) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForOneDimensional() + "one_dim_float.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 1);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::FLOAT);
    ASSERT_EQ(npyReader->getNumDimensions(), 1);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    auto row = (float*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1.0);
    ASSERT_EQ(row[1], 2.0);
    ASSERT_EQ(row[2], 3.0);

    row = (float*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 2.0);
    ASSERT_EQ(row[1], 3.0);

    row = (float*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 3.0);
}

TEST_F(NpyReaderTest, ReadNpyTwoDimensionalFloat) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForTwoDimensional() + "two_dim_float.npy");
    ASSERT_EQ(npyReader->getNumRows(), 3);
    ASSERT_EQ(npyReader->getNumElementsPerRow(), 3);
    ASSERT_EQ(npyReader->getType(), LogicalTypeID::FLOAT);
    ASSERT_EQ(npyReader->getNumDimensions(), 2);
    auto shape = npyReader->getShape();
    ASSERT_EQ(shape[0], 3);
    ASSERT_EQ(shape[1], 3);
    auto row = (float*)npyReader->getPointerToRow(0);
    ASSERT_EQ(row[0], 1.0);
    ASSERT_EQ(row[1], 2.0);
    ASSERT_EQ(row[2], 3.0);

    row = (float*)npyReader->getPointerToRow(1);
    ASSERT_EQ(row[0], 4.0);
    ASSERT_EQ(row[1], 5.0);
    ASSERT_EQ(row[2], 6.0);

    row = (float*)npyReader->getPointerToRow(2);
    ASSERT_EQ(row[0], 7.0);
    ASSERT_EQ(row[1], 8.0);
    ASSERT_EQ(row[2], 9.0);
}

TEST_F(NpyReaderTest, OutOfRangeIndex) {
    auto npyReader =
        std::make_unique<NpyReader>(getInputDirForOneDimensional() + "one_dim_float.npy");
    auto row = (float*)npyReader->getPointerToRow(333);
    ASSERT_EQ(row, nullptr);
}

TEST_F(NpyReaderTest, FileOpenError) {
    try {
        auto npyReader = std::make_unique<NpyReader>("does_not_exist.npy");
        FAIL();
    } catch (Exception e) { ASSERT_STREQ(e.what(), "Copy exception: Failed to open NPY file."); }
}

TEST_F(NpyReaderTest, FortranOrderError) {
    try {
        auto npyReader =
            std::make_unique<NpyReader>(getInputDirForOneDimensional() + "fortran_order.npy");
        FAIL();
    } catch (Exception e) {
        ASSERT_STREQ(
            e.what(), "Copy exception: Fortran-order NPY files are not currently supported.");
    }
}
