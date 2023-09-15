#!/bin/bash

# the root directory of the project
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../" >/dev/null 2>&1 && pwd )"

# download antlr4.jar if not exists
if [ ! -e antlr4.jar ]
then
    echo "Downloading antlr4.jar"
    curl --url 'https://www.antlr.org/download/antlr-4.13.1-complete.jar'\
         --output './antlr4.jar'
fi

# create the directory for the generated grammar
mkdir -p ./generated

# move grammar file to current directory
cp $ROOT_DIR/src/antlr4/Cypher.g4 ./Cypher.g4

# generate grammar
java -jar antlr4.jar -Dlanguage=Cpp -no-visitor -no-listener Cypher.g4 -o ./generated

# rename include path
sed 's/#include "CypherLexer.h"/#include "cypher_lexer.h"/g' ./generated/CypherLexer.cpp > ./generated/cypher_lexer.cpp
sed 's/#include "CypherParser.h"/#include "cypher_parser.h"/g' ./generated/CypherParser.cpp > ./generated/cypher_parser.cpp
mv ./generated/CypherLexer.h ./generated/cypher_lexer.h
mv ./generated/CypherParser.h ./generated/cypher_parser.h

# move generated files to the right place
mv ./generated/cypher_lexer.h $ROOT_DIR/third_party/antlr4_cypher/include/cypher_lexer.h
mv ./generated/cypher_lexer.cpp $ROOT_DIR/third_party/antlr4_cypher/cypher_lexer.cpp
mv ./generated/cypher_parser.h $ROOT_DIR/third_party/antlr4_cypher/include/cypher_parser.h
mv ./generated/cypher_parser.cpp $ROOT_DIR/third_party/antlr4_cypher/cypher_parser.cpp

# remove the generated directory
rm -rf ./generated

# remove the grammar file
rm ./Cypher.g4
