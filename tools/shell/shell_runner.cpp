
#include "tools/shell/include/embedded_shell.h"

using namespace std;

int main(int argc, char* argv[]) {
    if (argc <= 1 || argc >= 4) {
        fprintf(stderr, "Usage: %s <path to serialized dataset> [:IN-MEMORY].\n", argv[0]);
        exit(1);
    }
    auto serializedGraphPath = argv[1];
    bool isInMemoryMode = false;
    if (argc == 3) {
        string storageMode = argv[2];
        if (storageMode == ":IN-MEMORY") {
            isInMemoryMode = true;
        } else {
            fprintf(stderr, "Usage: %s <path to serialized dataset> [:IN-MEMORY].\n", argv[0]);
            exit(1);
        }
    }
    auto system = System(serializedGraphPath, isInMemoryMode);
    auto shell = EmbeddedShell(system);
    shell.initialize();
    shell.run();

    return 0;
}
