# Use a copy of the grammar file and compare since last run.
# This is to make sure clean build from source needn't have Java installed.
# We can't use checksums because of windows line ending normalization.
file(READ Cypher.g4.copy COPY_CONTENT)
file(READ ${ROOT_DIR}/src/antlr4/Cypher.g4 REAL_CONTENT)
file(READ keywords.txt.copy COPY_KEYWORDS)
file(READ ${ROOT_DIR}/src/antlr4/keywords.txt REAL_KEYWORDS)

if("${COPY_CONTENT}" STREQUAL "${REAL_CONTENT}" AND "${COPY_KEYWORDS}" STREQUAL "${REAL_KEYWORDS}")
    message(DEBUG " Not regenerating grammar files as Cypher.g4 is unchanged.")
    return() # Exit.
endif()

file(WRITE Cypher.g4.copy "${REAL_CONTENT}")
file(WRITE keywords.txt.copy "${REAL_KEYWORDS}")

message(INFO " Regenerating grammar files...")

if(NOT EXISTS antlr4.jar)
    message(INFO " Downloading antlr4.jar")
    file(
        DOWNLOAD https://www.antlr.org/download/antlr-4.13.1-complete.jar antlr4.jar
        EXPECTED_HASH SHA256=bc13a9c57a8dd7d5196888211e5ede657cb64a3ce968608697e4f668251a8487)
endif()

# create the directory for the generated grammar
file(MAKE_DIRECTORY generated)

find_package(Java REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

# use script to generate final Cypher.g4 file and update tools/shell/include/keywords.h
execute_process(COMMAND ${Python3_EXECUTABLE} keywordhandler.py ${ROOT_DIR}/src/antlr4/Cypher.g4 ${ROOT_DIR}/src/antlr4/keywords.txt Cypher.g4 ${ROOT_DIR}/tools/shell/include/keywords.h)

# Generate files.
message(INFO " Generating files...")
execute_process(COMMAND
    ${Java_JAVA_EXECUTABLE} -jar antlr4.jar -Dlanguage=Cpp -no-visitor -no-listener Cypher.g4 -o generated)

# Edit source files.
file(READ generated/CypherLexer.cpp LexerContent)
string(REPLACE "#include \"CypherLexer.h\"" "#include \"cypher_lexer.h\"" LexerReplacedContent "${LexerContent}")
file(WRITE ${ROOT_DIR}/third_party/antlr4_cypher/cypher_lexer.cpp "${LexerReplacedContent}")

file(READ generated/CypherParser.cpp ParserContent)
string(REPLACE "#include \"CypherParser.h\"" "#include \"cypher_parser.h\"" ParserReplacedContent "${ParserContent}")
file(WRITE ${ROOT_DIR}/third_party/antlr4_cypher/cypher_parser.cpp "${ParserReplacedContent}")

# Move headers.
file(RENAME generated/CypherParser.h ${ROOT_DIR}/third_party/antlr4_cypher/include/cypher_parser.h)
file(RENAME generated/CypherLexer.h ${ROOT_DIR}/third_party/antlr4_cypher/include/cypher_lexer.h)

# Cleanup
file(REMOVE_RECURSE generated)
