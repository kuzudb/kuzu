#include <iostream>
#include <string>
#include <set>
#include <random>
#include <fstream>


std::string generateRandomString(int length) {
    static const char characters[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static const int charactersCount = sizeof(characters) - 1;
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(0, charactersCount - 1);

    std::string randomString;
    for (int i = 0; i < length; ++i) {
        randomString += characters[distribution(generator)];
    }
    return randomString;
}

int main() {
    //int lengths[] = {10, 12, 14, 16, 20, 24, 30};
    int lengths[] = {30};
    //int percents[] = {20, 40, 60, 80};
    //int percents[] = {0, 100};
    for (int length: lengths) {
        int count = 10000000;
        std::ofstream outFile("csv/vPersonStringLengthPartialTest" + std::to_string(length) + ".csv");
        std::set<std::string> generatedStrings;

        /*
        while (generatedStrings.size() < count / 100 * percent) {
            int len = rand() % 12 + 1; 
            std::string randomString = generateRandomString(18);
            randomString = "abcdefghijkh" + randomString;
            generatedStrings.insert(randomString);
        }
        std::cout << "short done" << std::endl;
        */
        while (generatedStrings.size() < count) {
            std::string randomString = generateRandomString(18);
            randomString = "abcdefghijkh" + randomString;
            generatedStrings.insert(randomString);
        }

        /*
        std::vector<std::string> strVector(generatedStrings.begin(), generatedStrings.end());
        std::cout << "vector done" << std::endl;
        std::random_device rd;
        std::mt19937 rng(rd());
        std::shuffle(strVector.begin(), strVector.end(), rng);
        std::cout << "shuffle done" << std::endl;
        */

        for (const auto& str : generatedStrings) {
            outFile << str + "\n";
        }

        outFile.close();
    }

    return 0;
}
