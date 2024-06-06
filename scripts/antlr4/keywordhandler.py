import sys
# arg 1 is input Cypher.g4
inputCypher = open(sys.argv[1], 'r')
# arg 2 is input keywords.txt
inputKeywords = [x[:-1] for x in open(sys.argv[2], 'r')]
# arg 3 is output Cypher.g4
outputCypher = open(sys.argv[3], 'w')
# arg 4 is output keywords.h
outputKeywords = open(sys.argv[4], 'w')

print("Generating keywords for Cypher.g4")
antlr4String = ''.join(map(
    lambda x: x + ' : ' + ' '.join([f'( \'{i.upper()}\' | \'{i.lower()}\' )' if i != '_' else f'\'{i}\'' for i in x]) + ' ;\n\n',
    inputKeywords))

cypherPrefixString = '''
/*
* OpenCypher grammar at "https://s3.amazonaws.com/artifacts.opencypher.org/legacy/Cypher.g4"
*/
grammar Cypher;

// provide ad-hoc error messages for common syntax errors
@parser::declarations {
    virtual void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) {};
    virtual void notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token* startToken) {};
    virtual void notifyInvalidNotEqualOperator(antlr4::Token* startToken) {};
    virtual void notifyEmptyToken(antlr4::Token* startToken) {};
    virtual void notifyReturnNotAtEnd(antlr4::Token* startToken) {};
    virtual void notifyNonBinaryComparison(antlr4::Token* startToken) {};
}

'''

outputCypher.write(cypherPrefixString + antlr4String + '\n' + ''.join(inputCypher))
print("keywords for Cypher.g4 generated")

print("Generating keywordList for keywords.h")
asCList = '{"' + '", "'.join(inputKeywords) + '"}'

outputKeywords.write(f'''#ifndef _keywordList
// clang-format off
#define _keywordList {asCList}
#define _keywordListLength {len(inputKeywords)}
// clang-format on
#endif
''')
print("keywordList generated")
