
// Generated from /Users/xiyangfeng/kuzu/kuzu/src/antlr4/Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, COPY = 46, FROM = 47, NODE = 48, TABLE = 49, DROP = 50, 
    ALTER = 51, PRIMARY = 52, KEY = 53, REL = 54, TO = 55, EXPLAIN = 56, 
    PROFILE = 57, UNION = 58, ALL = 59, OPTIONAL = 60, MATCH = 61, UNWIND = 62, 
    CREATE = 63, SET = 64, DELETE = 65, WITH = 66, RETURN = 67, DISTINCT = 68, 
    STAR = 69, AS = 70, ORDER = 71, BY = 72, L_SKIP = 73, LIMIT = 74, ASCENDING = 75, 
    ASC = 76, DESCENDING = 77, DESC = 78, WHERE = 79, OR = 80, XOR = 81, 
    AND = 82, NOT = 83, INVALID_NOT_EQUAL = 84, MINUS = 85, FACTORIAL = 86, 
    STARTS = 87, ENDS = 88, CONTAINS = 89, IS = 90, NULL_ = 91, TRUE = 92, 
    FALSE = 93, EXISTS = 94, CASE = 95, ELSE = 96, END = 97, WHEN = 98, 
    THEN = 99, StringLiteral = 100, EscapedChar = 101, DecimalInteger = 102, 
    HexLetter = 103, HexDigit = 104, Digit = 105, NonZeroDigit = 106, NonZeroOctDigit = 107, 
    ZeroDigit = 108, RegularDecimalReal = 109, UnescapedSymbolicName = 110, 
    IdentifierStart = 111, IdentifierPart = 112, EscapedSymbolicName = 113, 
    SP = 114, WHITESPACE = 115, Comment = 116, Unknown = 117
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

