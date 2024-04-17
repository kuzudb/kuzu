
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, ATTACH = 46, DBTYPE = 47, USE = 48, CALL = 49, COMMENT_ = 50, 
    MACRO = 51, GLOB = 52, COPY = 53, FROM = 54, COLUMN = 55, EXPORT = 56, 
    IMPORT = 57, DATABASE = 58, NODE = 59, TABLE = 60, GROUP = 61, RDFGRAPH = 62, 
    DROP = 63, ALTER = 64, DEFAULT = 65, RENAME = 66, ADD = 67, PRIMARY = 68, 
    KEY = 69, REL = 70, TO = 71, EXPLAIN = 72, PROFILE = 73, BEGIN = 74, 
    TRANSACTION = 75, READ = 76, ONLY = 77, WRITE = 78, COMMIT = 79, COMMIT_SKIP_CHECKPOINT = 80, 
    ROLLBACK = 81, ROLLBACK_SKIP_CHECKPOINT = 82, INSTALL = 83, EXTENSION = 84, 
    UNION = 85, ALL = 86, LOAD = 87, HEADERS = 88, OPTIONAL = 89, MATCH = 90, 
    UNWIND = 91, CREATE = 92, MERGE = 93, ON = 94, SET = 95, DETACH = 96, 
    DELETE = 97, WITH = 98, RETURN = 99, DISTINCT = 100, STAR = 101, AS = 102, 
    ORDER = 103, BY = 104, L_SKIP = 105, LIMIT = 106, ASCENDING = 107, ASC = 108, 
    DESCENDING = 109, DESC = 110, WHERE = 111, SHORTEST = 112, OR = 113, 
    XOR = 114, AND = 115, NOT = 116, INVALID_NOT_EQUAL = 117, MINUS = 118, 
    FACTORIAL = 119, COLON = 120, IN = 121, STARTS = 122, ENDS = 123, CONTAINS = 124, 
    IS = 125, NULL_ = 126, TRUE = 127, FALSE = 128, COUNT = 129, EXISTS = 130, 
    CASE = 131, ELSE = 132, END = 133, WHEN = 134, THEN = 135, StringLiteral = 136, 
    EscapedChar = 137, DecimalInteger = 138, HexLetter = 139, HexDigit = 140, 
    Digit = 141, NonZeroDigit = 142, NonZeroOctDigit = 143, ZeroDigit = 144, 
    RegularDecimalReal = 145, UnescapedSymbolicName = 146, IdentifierStart = 147, 
    IdentifierPart = 148, EscapedSymbolicName = 149, SP = 150, WHITESPACE = 151, 
    Comment = 152, Unknown = 153
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

