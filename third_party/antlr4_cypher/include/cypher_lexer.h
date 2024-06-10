
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
<<<<<<< HEAD
    IMPORT = 57, DATABASE = 58, NODE = 59, TABLE = 60, GROUP = 61, RDFGRAPH = 62, 
    SEQUENCE = 63, TYPE = 64, INCREMENT = 65, MINVALUE = 66, MAXVALUE = 67, 
    START = 68, NO = 69, CYCLE = 70, DROP = 71, ALTER = 72, DEFAULT = 73, 
    RENAME = 74, ADD = 75, PRIMARY = 76, KEY = 77, REL = 78, TO = 79, DECIMAL = 80, 
    EXPLAIN = 81, PROFILE = 82, BEGIN = 83, TRANSACTION = 84, READ = 85, 
    ONLY = 86, WRITE = 87, COMMIT = 88, COMMIT_SKIP_CHECKPOINT = 89, ROLLBACK = 90, 
    ROLLBACK_SKIP_CHECKPOINT = 91, INSTALL = 92, EXTENSION = 93, UNION = 94, 
    ALL = 95, LOAD = 96, HEADERS = 97, PROJECT = 98, GRAPH = 99, OPTIONAL = 100, 
    MATCH = 101, UNWIND = 102, CREATE = 103, MERGE = 104, ON = 105, SET = 106, 
    DETACH = 107, DELETE = 108, WITH = 109, RETURN = 110, DISTINCT = 111, 
    STAR = 112, AS = 113, ORDER = 114, BY = 115, L_SKIP = 116, LIMIT = 117, 
    ASCENDING = 118, ASC = 119, DESCENDING = 120, DESC = 121, WHERE = 122, 
    SHORTEST = 123, OR = 124, XOR = 125, AND = 126, NOT = 127, INVALID_NOT_EQUAL = 128, 
    MINUS = 129, FACTORIAL = 130, COLON = 131, IN = 132, STARTS = 133, ENDS = 134, 
    CONTAINS = 135, IS = 136, NULL_ = 137, TRUE = 138, FALSE = 139, CAST = 140, 
    COUNT = 141, EXISTS = 142, CASE = 143, ELSE = 144, END = 145, WHEN = 146, 
    THEN = 147, StringLiteral = 148, EscapedChar = 149, DecimalInteger = 150, 
    HexLetter = 151, HexDigit = 152, Digit = 153, NonZeroDigit = 154, NonZeroOctDigit = 155, 
    ZeroDigit = 156, RegularDecimalReal = 157, UnescapedSymbolicName = 158, 
    IdentifierStart = 159, IdentifierPart = 160, EscapedSymbolicName = 161, 
    SP = 162, WHITESPACE = 163, Comment = 164, Unknown = 165
=======
    IMPORT = 57, DATABASE = 58, IF = 59, NODE = 60, TABLE = 61, GROUP = 62, 
    RDFGRAPH = 63, SEQUENCE = 64, TYPE = 65, INCREMENT = 66, MINVALUE = 67, 
    MAXVALUE = 68, START = 69, NO = 70, CYCLE = 71, DROP = 72, ALTER = 73, 
    DEFAULT = 74, RENAME = 75, ADD = 76, PRIMARY = 77, KEY = 78, REL = 79, 
    TO = 80, DECIMAL = 81, EXPLAIN = 82, PROFILE = 83, BEGIN = 84, TRANSACTION = 85, 
    READ = 86, ONLY = 87, WRITE = 88, COMMIT = 89, COMMIT_SKIP_CHECKPOINT = 90, 
    ROLLBACK = 91, ROLLBACK_SKIP_CHECKPOINT = 92, INSTALL = 93, EXTENSION = 94, 
    UNION = 95, ALL = 96, LOAD = 97, HEADERS = 98, OPTIONAL = 99, MATCH = 100, 
    UNWIND = 101, CREATE = 102, MERGE = 103, ON = 104, SET = 105, DETACH = 106, 
    DELETE = 107, WITH = 108, RETURN = 109, DISTINCT = 110, STAR = 111, 
    AS = 112, ORDER = 113, BY = 114, L_SKIP = 115, LIMIT = 116, ASCENDING = 117, 
    ASC = 118, DESCENDING = 119, DESC = 120, WHERE = 121, SHORTEST = 122, 
    OR = 123, XOR = 124, AND = 125, NOT = 126, INVALID_NOT_EQUAL = 127, 
    MINUS = 128, FACTORIAL = 129, COLON = 130, IN = 131, STARTS = 132, ENDS = 133, 
    CONTAINS = 134, IS = 135, NULL_ = 136, TRUE = 137, FALSE = 138, COUNT = 139, 
    EXISTS = 140, CASE = 141, ELSE = 142, END = 143, WHEN = 144, THEN = 145, 
    StringLiteral = 146, EscapedChar = 147, DecimalInteger = 148, HexLetter = 149, 
    HexDigit = 150, Digit = 151, NonZeroDigit = 152, NonZeroOctDigit = 153, 
    ZeroDigit = 154, RegularDecimalReal = 155, UnescapedSymbolicName = 156, 
    IdentifierStart = 157, IdentifierPart = 158, EscapedSymbolicName = 159, 
    SP = 160, WHITESPACE = 161, Comment = 162, Unknown = 163
>>>>>>> 88adc2165 (update)
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

