#include "common/vector_index/helpers.h"
#include "storage/index/quantization.h"
#include "common/vector_index/distance_computer.h"
#include "gtest/gtest.h"

using namespace kuzu::common;
using namespace kuzu::storage;

TEST(NormalizeVector, NormalizeVector) {
    std::vector<float> vec = {1, 2, 3, 4, 5};
    std::vector<float> actualNorm = {0.13483997249264842, 0.26967994498529685, 0.4045199174779453, 0.5393598899705937,
                                     0.6741998624632421};
    // calculate l2 norm
    float l2Norm = 0;
    for (size_t i = 0; i < vec.size(); i++) {
        l2Norm += vec[i] * vec[i];
    }
    l2Norm = sqrt(l2Norm);
    // normalize vector
    for (size_t i = 0; i < vec.size(); i++) {
        actualNorm[i] = vec[i] / l2Norm;
    }

    float *normalizedVec = new float[vec.size()];
    normalize_vectors(vec.data(), vec.size(), normalizedVec);
    for (size_t i = 0; i < vec.size(); i++) {
        printf("%f ", normalizedVec[i]);
    }
    printf("\n");
    for (size_t i = 0; i < vec.size(); i++) {
        printf("%f ", actualNorm[i]);
    }
}

TEST(QuantizationTest, QuantizationTest) {
    size_t q, qd;
    float *qVecs = readFvecFile("/Users/gauravsehgal/work/orangedb/data/gist_10k/query.fvecs", &qd, &q);

    size_t numVecs, dim;
    float *vecs = readFvecFile("/Users/gauravsehgal/work/orangedb/data/gist_10k/base.fvecs", &dim, &numVecs);
    float *normalizedVecs = new float[numVecs * dim];
    for (size_t i = 0; i < numVecs; i++) {
        normalize_vectors(vecs + i * dim, dim, normalizedVecs + i * dim);
    }

    SQ8Bit sq(dim);
    uint8_t *codes = new uint8_t[numVecs * sq.codeSize];
    sq.batchTrain(normalizedVecs, numVecs);
    sq.finalizeTrain();
    sq.encode(normalizedVecs, codes, numVecs);

    float *decodedVecs = new float[numVecs * dim];
    sq.decode(codes, decodedVecs, numVecs);

    // print the first vector from both normalized and decoded vectors
    for (size_t i = dim; i < dim + 10; i++) {
        printf("%f ", normalizedVecs[i]);
    }
    printf("\n");
    for (size_t i = dim; i < dim + 10; i++) {
        printf("%f ", decodedVecs[i]);
    }
    printf("\n");

    double expectedDist, actualDist;
    SQ8AsymCosine sqAsym(dim, sq.getAlpha(), sq.getBeta(), sq.getAlphaSqr());
    sqAsym.setQuery(qVecs);
    sqAsym.computeDistance(codes, &actualDist);

    CosineDistanceComputer dc(dim);
    dc.setQuery(qVecs);
    dc.computeDistance(vecs, &expectedDist);

    printf("Expected distance: %f\n", expectedDist);
    printf("Actual distance: %f\n", actualDist);

}