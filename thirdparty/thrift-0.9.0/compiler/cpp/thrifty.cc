/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.0.4"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* Copy the first part of user declarations.  */
#line 1 "thrifty.yy" /* yacc.c:339  */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Thrift parser.
 *
 * This parser is used on a thrift definition file.
 *
 */

#define __STDC_LIMIT_MACROS
#define __STDC_FORMAT_MACROS
#include <stdio.h>
#ifndef _MSC_VER
#include <inttypes.h>
#else
#include <stdint.h>
#endif
#include <limits.h>
#include "main.h"
#include "globals.h"
#include "parse/t_program.h"
#include "parse/t_scope.h"

/**
 * This global variable is used for automatic numbering of field indices etc.
 * when parsing the members of a struct. Field values are automatically
 * assigned starting from -1 and working their way down.
 */
int y_field_val = -1;
int g_arglist = 0;
const int struct_is_struct = 0;
const int struct_is_union = 1;


#line 119 "thrifty.cc" /* yacc.c:339  */

# ifndef YY_NULLPTR
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULLPTR nullptr
#  else
#   define YY_NULLPTR 0
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* In a future release of Bison, this section will be replaced
   by #include "y.tab.h".  */
#ifndef YY_YY_Y_TAB_H_INCLUDED
# define YY_YY_Y_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    tok_identifier = 258,
    tok_literal = 259,
    tok_doctext = 260,
    tok_st_identifier = 261,
    tok_int_constant = 262,
    tok_dub_constant = 263,
    tok_include = 264,
    tok_namespace = 265,
    tok_cpp_namespace = 266,
    tok_cpp_include = 267,
    tok_cpp_type = 268,
    tok_php_namespace = 269,
    tok_py_module = 270,
    tok_perl_package = 271,
    tok_java_package = 272,
    tok_xsd_all = 273,
    tok_xsd_optional = 274,
    tok_xsd_nillable = 275,
    tok_xsd_namespace = 276,
    tok_xsd_attrs = 277,
    tok_ruby_namespace = 278,
    tok_smalltalk_category = 279,
    tok_smalltalk_prefix = 280,
    tok_cocoa_prefix = 281,
    tok_csharp_namespace = 282,
    tok_delphi_namespace = 283,
    tok_void = 284,
    tok_bool = 285,
    tok_byte = 286,
    tok_string = 287,
    tok_binary = 288,
    tok_slist = 289,
    tok_senum = 290,
    tok_i16 = 291,
    tok_i32 = 292,
    tok_i64 = 293,
    tok_double = 294,
    tok_map = 295,
    tok_list = 296,
    tok_set = 297,
    tok_oneway = 298,
    tok_typedef = 299,
    tok_struct = 300,
    tok_xception = 301,
    tok_throws = 302,
    tok_extends = 303,
    tok_service = 304,
    tok_enum = 305,
    tok_const = 306,
    tok_required = 307,
    tok_optional = 308,
    tok_union = 309
  };
#endif
/* Tokens.  */
#define tok_identifier 258
#define tok_literal 259
#define tok_doctext 260
#define tok_st_identifier 261
#define tok_int_constant 262
#define tok_dub_constant 263
#define tok_include 264
#define tok_namespace 265
#define tok_cpp_namespace 266
#define tok_cpp_include 267
#define tok_cpp_type 268
#define tok_php_namespace 269
#define tok_py_module 270
#define tok_perl_package 271
#define tok_java_package 272
#define tok_xsd_all 273
#define tok_xsd_optional 274
#define tok_xsd_nillable 275
#define tok_xsd_namespace 276
#define tok_xsd_attrs 277
#define tok_ruby_namespace 278
#define tok_smalltalk_category 279
#define tok_smalltalk_prefix 280
#define tok_cocoa_prefix 281
#define tok_csharp_namespace 282
#define tok_delphi_namespace 283
#define tok_void 284
#define tok_bool 285
#define tok_byte 286
#define tok_string 287
#define tok_binary 288
#define tok_slist 289
#define tok_senum 290
#define tok_i16 291
#define tok_i32 292
#define tok_i64 293
#define tok_double 294
#define tok_map 295
#define tok_list 296
#define tok_set 297
#define tok_oneway 298
#define tok_typedef 299
#define tok_struct 300
#define tok_xception 301
#define tok_throws 302
#define tok_extends 303
#define tok_service 304
#define tok_enum 305
#define tok_const 306
#define tok_required 307
#define tok_optional 308
#define tok_union 309

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 58 "thrifty.yy" /* yacc.c:355  */

  char*          id;
  int64_t        iconst;
  double         dconst;
  bool           tbool;
  t_doc*         tdoc;
  t_type*        ttype;
  t_base_type*   tbase;
  t_typedef*     ttypedef;
  t_enum*        tenum;
  t_enum_value*  tenumv;
  t_const*       tconst;
  t_const_value* tconstv;
  t_struct*      tstruct;
  t_service*     tservice;
  t_function*    tfunction;
  t_field*       tfield;
  char*          dtext;
  t_field::e_req ereq;
  t_annotation*  tannot;
  t_field_id     tfieldid;

#line 290 "thrifty.cc" /* yacc.c:355  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;

int yyparse (void);

#endif /* !YY_YY_Y_TAB_H_INCLUDED  */

/* Copy the second part of user declarations.  */

#line 307 "thrifty.cc" /* yacc.c:358  */

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

#if !defined _Noreturn \
     && (!defined __STDC_VERSION__ || __STDC_VERSION__ < 201112)
# if defined _MSC_VER && 1200 <= _MSC_VER
#  define _Noreturn __declspec (noreturn)
# else
#  define _Noreturn YY_ATTRIBUTE ((__noreturn__))
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif


#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYSIZE_T yynewbytes;                                            \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / sizeof (*yyptr);                          \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, (Count) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYSIZE_T yyi;                         \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  3
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   201

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  68
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  57
/* YYNRULES -- Number of rules.  */
#define YYNRULES  122
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  220

/* YYTRANSLATE[YYX] -- Symbol number corresponding to YYX as returned
   by yylex, with out-of-bounds checking.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   309

#define YYTRANSLATE(YYX)                                                \
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, without out-of-bounds checking.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      64,    65,    55,     2,    56,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    63,    57,
      66,    60,    67,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    61,     2,    62,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    58,     2,    59,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   235,   235,   248,   259,   268,   273,   278,   282,   289,
     297,   305,   312,   321,   330,   339,   348,   357,   366,   375,
     384,   393,   402,   412,   424,   432,   437,   445,   456,   470,
     477,   484,   491,   498,   507,   519,   521,   524,   527,   554,
     561,   567,   586,   600,   611,   618,   625,   632,   650,   659,
     665,   670,   676,   681,   688,   695,   702,   709,   716,   723,
     730,   734,   740,   757,   762,   767,   772,   777,   782,   787,
     792,   797,   810,   823,   828,   833,   846,   851,   858,   864,
     878,   883,   888,   898,   903,   913,   920,   952,   988,   994,
     998,  1010,  1015,  1024,  1029,  1034,  1041,  1056,  1061,  1067,
    1080,  1085,  1090,  1095,  1100,  1105,  1110,  1115,  1120,  1126,
    1137,  1142,  1147,  1154,  1164,  1174,  1184,  1189,  1194,  1200,
    1205,  1213,  1219
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 0
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "tok_identifier", "tok_literal",
  "tok_doctext", "tok_st_identifier", "tok_int_constant",
  "tok_dub_constant", "tok_include", "tok_namespace", "tok_cpp_namespace",
  "tok_cpp_include", "tok_cpp_type", "tok_php_namespace", "tok_py_module",
  "tok_perl_package", "tok_java_package", "tok_xsd_all",
  "tok_xsd_optional", "tok_xsd_nillable", "tok_xsd_namespace",
  "tok_xsd_attrs", "tok_ruby_namespace", "tok_smalltalk_category",
  "tok_smalltalk_prefix", "tok_cocoa_prefix", "tok_csharp_namespace",
  "tok_delphi_namespace", "tok_void", "tok_bool", "tok_byte", "tok_string",
  "tok_binary", "tok_slist", "tok_senum", "tok_i16", "tok_i32", "tok_i64",
  "tok_double", "tok_map", "tok_list", "tok_set", "tok_oneway",
  "tok_typedef", "tok_struct", "tok_xception", "tok_throws", "tok_extends",
  "tok_service", "tok_enum", "tok_const", "tok_required", "tok_optional",
  "tok_union", "'*'", "','", "';'", "'{'", "'}'", "'='", "'['", "']'",
  "':'", "'('", "')'", "'<'", "'>'", "$accept", "Program",
  "CaptureDocText", "DestroyDocText", "HeaderList", "Header", "Include",
  "DefinitionList", "Definition", "TypeDefinition", "Typedef",
  "CommaOrSemicolonOptional", "Enum", "EnumDefList", "EnumDef", "Senum",
  "SenumDefList", "SenumDef", "Const", "ConstValue", "ConstList",
  "ConstListContents", "ConstMap", "ConstMapContents", "StructHead",
  "Struct", "XsdAll", "XsdOptional", "XsdNillable", "XsdAttributes",
  "Xception", "Service", "FlagArgs", "UnflagArgs", "Extends",
  "FunctionList", "Function", "Oneway", "Throws", "FieldList", "Field",
  "FieldIdentifier", "FieldRequiredness", "FieldValue", "FunctionType",
  "FieldType", "BaseType", "SimpleBaseType", "ContainerType",
  "SimpleContainerType", "MapType", "SetType", "ListType", "CppType",
  "TypeAnnotations", "TypeAnnotationList", "TypeAnnotation", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,    42,    44,    59,   123,   125,
      61,    91,    93,    58,    40,    41,    60,    62
};
# endif

#define YYPACT_NINF -137

#define yypact_value_is_default(Yystate) \
  (!!((Yystate) == (-137)))

#define YYTABLE_NINF -75

#define yytable_value_is_error(Yytable_value) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
    -137,    23,    78,  -137,   161,    30,    27,    19,    29,    31,
      33,    34,    36,    37,    39,    38,    40,    41,    44,    48,
      49,  -137,  -137,    42,  -137,    50,    54,  -137,  -137,  -137,
    -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,
      55,   127,  -137,    56,    72,    79,   127,  -137,  -137,  -137,
    -137,  -137,  -137,  -137,    80,  -137,  -137,  -137,  -137,  -137,
     -25,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,
    -137,    68,    28,    68,    86,  -137,    35,  -137,    35,  -137,
    -137,  -137,    46,    47,    51,    95,    82,  -137,    98,    45,
     127,    52,    35,  -137,  -137,  -137,  -137,   102,    57,  -137,
      59,  -137,    63,    12,  -137,   127,    43,   127,  -137,     2,
      53,  -137,  -137,    58,     5,  -137,   -30,    35,  -137,    69,
      68,    66,    74,  -137,  -137,    35,   107,  -137,  -137,    35,
     123,  -137,  -137,  -137,  -137,  -137,  -137,  -137,   -30,  -137,
    -137,    81,  -137,  -137,  -137,  -137,   127,  -137,  -137,   133,
    -137,    75,   -24,    97,  -137,   -40,    11,     3,  -137,    35,
     112,   -30,  -137,  -137,  -137,   127,    96,   103,  -137,   134,
     -30,  -137,   111,  -137,   -30,  -137,  -137,  -137,   177,  -137,
     113,    35,    35,  -137,     5,  -137,   121,  -137,   180,  -137,
    -137,   -30,   -30,     5,   171,   128,  -137,  -137,  -137,  -137,
     173,  -137,  -137,   169,   129,   137,    35,   149,  -137,   -30,
     135,    35,   138,  -137,  -137,   -30,  -137,   136,  -137,  -137
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       6,     0,     4,     1,     0,     3,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     5,     7,     0,    23,     0,     0,    10,    11,    12,
      13,    14,    18,    20,    15,    16,    17,    19,    21,    22,
       0,     0,    60,     0,     0,     0,     0,    61,    24,    27,
      29,    30,    31,    26,     0,    32,    33,    28,     8,     9,
       0,    96,   103,   104,   100,   101,   102,   105,   106,   107,
     108,   117,     0,   117,     0,    97,   119,    98,   119,   110,
     111,   112,     0,    76,     0,     0,    64,    45,     0,     0,
       0,     0,   119,   121,    99,   109,    85,     0,     0,    40,
       0,    63,     0,     0,   116,     0,     0,     0,    34,     0,
       3,    75,    73,     3,     0,    85,    37,   119,    44,     0,
     117,     0,     0,   118,   120,   119,    88,    84,    78,   119,
       0,    39,    51,    50,    48,    49,    59,    56,    37,    52,
      53,     3,    35,    36,    46,    43,     0,   115,   114,     0,
      71,     0,    91,     3,    38,   119,     0,     0,    47,   119,
       0,    37,    87,    89,    90,     0,    81,     0,    77,     0,
      37,    57,     0,    54,    37,    62,   113,   122,     0,    80,
       0,   119,   119,    42,     0,    55,    93,    95,     0,    94,
      72,    37,    37,     0,    66,     0,    41,    58,    92,    65,
      68,    85,    67,    70,     3,     0,   119,    83,    85,    37,
       0,   119,     3,    86,    85,    37,    69,     3,    79,    82
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -137,  -137,    -5,  -137,  -137,  -137,  -137,  -137,  -137,  -137,
    -137,  -136,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -108,
    -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,
    -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -137,  -111,
    -137,  -137,  -137,  -137,  -137,   -45,  -137,  -137,  -137,  -137,
    -137,  -137,  -137,   -52,   -75,  -137,  -137
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,     1,   126,     4,     2,    21,    22,     5,    48,    49,
      50,   144,    51,   113,   131,    52,   103,   118,    53,   138,
     139,   157,   140,   156,    54,    55,   102,   200,   203,   206,
      56,    57,   128,   167,    98,   153,   168,   180,   211,   110,
     127,   152,   165,   194,   188,    74,    75,    76,    77,    78,
      79,    80,    81,    89,    94,   109,   124
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      23,    85,   158,    95,   141,   122,   132,   133,   132,   133,
     134,   135,   134,   135,   132,   133,   116,   108,   134,   135,
     169,    91,    25,     3,    93,   177,   142,   143,   163,   164,
      -2,    24,    27,    87,   183,    28,    29,    30,   185,    31,
      32,    34,   145,    33,    36,   106,    35,    37,   172,   174,
     150,    38,    39,    58,   154,   196,   197,    59,    60,    82,
     119,   136,   121,   136,   137,   173,   137,   123,   147,   136,
     171,   117,   137,   213,    26,    83,   192,    40,   -25,   218,
     170,    88,    84,    86,   175,   198,    41,    42,    43,    92,
     204,    44,    45,    46,    90,    97,    47,   212,   100,    93,
     101,   160,   104,   217,    96,   111,   190,   191,   130,    99,
     120,   105,   125,   -25,   151,   112,    61,   129,   107,   114,
     178,   115,   -25,   -25,   -25,   146,   155,   -25,   -25,   -25,
      61,   209,   -25,   148,   149,   189,   215,   161,   162,   179,
     159,   182,   187,    62,    63,    64,    65,    66,   166,    67,
      68,    69,    70,    71,    72,    73,   -74,    62,    63,    64,
      65,    66,   181,    67,    68,    69,    70,    71,    72,    73,
       6,     7,     8,     9,   184,    10,    11,    12,    13,   176,
     186,   193,    14,   195,    15,    16,    17,    18,    19,    20,
     199,   205,   201,   202,   207,   208,   210,   216,     0,   214,
       0,   219
};

static const yytype_int16 yycheck[] =
{
       5,    46,   138,    78,   115,     3,     3,     4,     3,     4,
       7,     8,     7,     8,     3,     4,     4,    92,     7,     8,
      60,    73,     3,     0,    64,   161,    56,    57,    52,    53,
       0,     4,     3,    58,   170,     4,     3,     3,   174,     3,
       3,     3,   117,     4,     3,    90,     6,     3,   156,   157,
     125,     3,     3,     3,   129,   191,   192,     3,     3,     3,
     105,    58,   107,    58,    61,    62,    61,    65,   120,    58,
      59,    59,    61,   209,    55,     3,   184,    35,     0,   215,
     155,    13,     3,     3,   159,   193,    44,    45,    46,     3,
     201,    49,    50,    51,    66,    48,    54,   208,     3,    64,
      18,   146,     4,   214,    58,     3,   181,   182,   113,    58,
      67,    66,    59,    35,     7,    58,     3,    59,    66,    60,
     165,    58,    44,    45,    46,    56,     3,    49,    50,    51,
       3,   206,    54,    67,    60,   180,   211,     4,    63,    43,
      59,     7,    29,    30,    31,    32,    33,    34,   153,    36,
      37,    38,    39,    40,    41,    42,    59,    30,    31,    32,
      33,    34,    59,    36,    37,    38,    39,    40,    41,    42,
       9,    10,    11,    12,    63,    14,    15,    16,    17,    67,
       3,    60,    21,     3,    23,    24,    25,    26,    27,    28,
      19,    22,    64,    20,    65,    58,    47,    59,    -1,    64,
      -1,    65
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    69,    72,     0,    71,    75,     9,    10,    11,    12,
      14,    15,    16,    17,    21,    23,    24,    25,    26,    27,
      28,    73,    74,    70,     4,     3,    55,     3,     4,     3,
       3,     3,     3,     4,     3,     6,     3,     3,     3,     3,
      35,    44,    45,    46,    49,    50,    51,    54,    76,    77,
      78,    80,    83,    86,    92,    93,    98,    99,     3,     3,
       3,     3,    30,    31,    32,    33,    34,    36,    37,    38,
      39,    40,    41,    42,   113,   114,   115,   116,   117,   118,
     119,   120,     3,     3,     3,   113,     3,    58,    13,   121,
      66,   121,     3,    64,   122,   122,    58,    48,   102,    58,
       3,    18,    94,    84,     4,    66,   113,    66,   122,   123,
     107,     3,    58,    81,    60,    58,     4,    59,    85,   113,
      67,   113,     3,    65,   124,    59,    70,   108,   100,    59,
      70,    82,     3,     4,     7,     8,    58,    61,    87,    88,
      90,   107,    56,    57,    79,   122,    56,   121,    67,    60,
     122,     7,   109,   103,   122,     3,    91,    89,    79,    59,
     113,     4,    63,    52,    53,   110,    70,   101,   104,    60,
     122,    59,    87,    62,    87,   122,    67,    79,   113,    43,
     105,    59,     7,    79,    63,    79,     3,    29,   112,   113,
     122,   122,    87,    60,   111,     3,    79,    79,    87,    19,
      95,    64,    20,    96,   107,    22,    97,    65,    58,   122,
      47,   106,   107,    79,    64,   122,    59,   107,    79,    65
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    68,    69,    70,    71,    72,    72,    73,    73,    73,
      73,    73,    73,    73,    73,    73,    73,    73,    73,    73,
      73,    73,    73,    74,    75,    75,    76,    76,    76,    77,
      77,    77,    77,    77,    78,    79,    79,    79,    80,    81,
      81,    82,    82,    83,    84,    84,    85,    86,    87,    87,
      87,    87,    87,    87,    88,    89,    89,    90,    91,    91,
      92,    92,    93,    94,    94,    95,    95,    96,    96,    97,
      97,    98,    99,   100,   101,   102,   102,   103,   103,   104,
     105,   105,   106,   106,   107,   107,   108,   109,   109,   110,
     110,   110,   111,   111,   112,   112,   113,   113,   113,   114,
     115,   115,   115,   115,   115,   115,   115,   115,   115,   116,
     117,   117,   117,   118,   119,   120,   121,   121,   122,   122,
     123,   123,   124
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     0,     0,     3,     0,     1,     3,     3,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     3,     0,     1,     1,     1,     1,
       1,     1,     1,     1,     4,     1,     1,     0,     6,     2,
       0,     6,     4,     6,     2,     0,     2,     6,     1,     1,
       1,     1,     1,     1,     3,     3,     0,     3,     5,     0,
       1,     1,     7,     1,     0,     1,     0,     1,     0,     4,
       0,     6,     9,     0,     0,     2,     0,     2,     0,    10,
       1,     0,     4,     0,     2,     0,    11,     2,     0,     1,
       1,     0,     2,     0,     1,     1,     1,     1,     1,     2,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     2,
       1,     1,     1,     7,     5,     5,     2,     0,     3,     0,
       2,     0,     4
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                  \
do                                                              \
  if (yychar == YYEMPTY)                                        \
    {                                                           \
      yychar = (Token);                                         \
      yylval = (Value);                                         \
      YYPOPSTACK (yylen);                                       \
      yystate = *yyssp;                                         \
      goto yybackup;                                            \
    }                                                           \
  else                                                          \
    {                                                           \
      yyerror (YY_("syntax error: cannot back up")); \
      YYERROR;                                                  \
    }                                                           \
while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*----------------------------------------.
| Print this symbol's value on YYOUTPUT.  |
`----------------------------------------*/

static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
{
  FILE *yyo = yyoutput;
  YYUSE (yyo);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# endif
  YYUSE (yytype);
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyoutput, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyoutput, yytype, yyvaluep);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yytype_int16 *yyssp, YYSTYPE *yyvsp, int yyrule)
{
  unsigned long int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[yyssp[yyi + 1 - yynrhs]],
                       &(yyvsp[(yyi + 1) - (yynrhs)])
                                              );
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
yystrlen (const char *yystr)
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            /* Fall through.  */
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYSIZE_T yysize1 = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (! (yysize <= yysize1
                         && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                    return 2;
                  yysize = yysize1;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    YYSIZE_T yysize1 = yysize + yystrlen (yyformat);
    if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
      return 2;
    yysize = yysize1;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep)
{
  YYUSE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;


/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        YYSTYPE *yyvs1 = yyvs;
        yytype_int16 *yyss1 = yyss;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * sizeof (*yyssp),
                    &yyvs1, yysize * sizeof (*yyvsp),
                    &yystacksize);

        yyss = yyss1;
        yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yytype_int16 *yyss1 = yyss;
        union yyalloc *yyptr =
          (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
                  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex ();
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:
#line 236 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Program -> Headers DefinitionList");
      /*
      TODO(dreiss): Decide whether full-program doctext is worth the trouble.
      if ($1 != NULL) {
        g_program->set_doc($1);
      }
      */
      clear_doctext();
    }
#line 1566 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 3:
#line 248 "thrifty.yy" /* yacc.c:1646  */
    {
      if (g_parse_mode == PROGRAM) {
        (yyval.dtext) = g_doctext;
        g_doctext = NULL;
      } else {
        (yyval.dtext) = NULL;
      }
    }
#line 1579 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 4:
#line 259 "thrifty.yy" /* yacc.c:1646  */
    {
      if (g_parse_mode == PROGRAM) {
        clear_doctext();
      }
    }
#line 1589 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 5:
#line 269 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("HeaderList -> HeaderList Header");
    }
#line 1597 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 6:
#line 273 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("HeaderList -> ");
    }
#line 1605 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 7:
#line 279 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Header -> Include");
    }
#line 1613 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 8:
#line 283 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Header -> tok_namespace tok_identifier tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace((yyvsp[-1].id), (yyvsp[0].id));
      }
    }
#line 1624 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 9:
#line 290 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Header -> tok_namespace * tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("*", (yyvsp[0].id));
      }
    }
#line 1635 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 10:
#line 298 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'cpp_namespace' is deprecated. Use 'namespace cpp' instead");
      pdebug("Header -> tok_cpp_namespace tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("cpp", (yyvsp[0].id));
      }
    }
#line 1647 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 11:
#line 306 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Header -> tok_cpp_include tok_literal");
      if (g_parse_mode == PROGRAM) {
        g_program->add_cpp_include((yyvsp[0].id));
      }
    }
#line 1658 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 12:
#line 313 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'php_namespace' is deprecated. Use 'namespace php' instead");
      pdebug("Header -> tok_php_namespace tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("php", (yyvsp[0].id));
      }
    }
#line 1670 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 13:
#line 322 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'py_module' is deprecated. Use 'namespace py' instead");
      pdebug("Header -> tok_py_module tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("py", (yyvsp[0].id));
      }
    }
#line 1682 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 14:
#line 331 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'perl_package' is deprecated. Use 'namespace perl' instead");
      pdebug("Header -> tok_perl_namespace tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("perl", (yyvsp[0].id));
      }
    }
#line 1694 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 15:
#line 340 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'ruby_namespace' is deprecated. Use 'namespace rb' instead");
      pdebug("Header -> tok_ruby_namespace tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("rb", (yyvsp[0].id));
      }
    }
#line 1706 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 16:
#line 349 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'smalltalk_category' is deprecated. Use 'namespace smalltalk.category' instead");
      pdebug("Header -> tok_smalltalk_category tok_st_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("smalltalk.category", (yyvsp[0].id));
      }
    }
#line 1718 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 17:
#line 358 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'smalltalk_prefix' is deprecated. Use 'namespace smalltalk.prefix' instead");
      pdebug("Header -> tok_smalltalk_prefix tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("smalltalk.prefix", (yyvsp[0].id));
      }
    }
#line 1730 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 18:
#line 367 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'java_package' is deprecated. Use 'namespace java' instead");
      pdebug("Header -> tok_java_package tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("java", (yyvsp[0].id));
      }
    }
#line 1742 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 19:
#line 376 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'cocoa_prefix' is deprecated. Use 'namespace cocoa' instead");
      pdebug("Header -> tok_cocoa_prefix tok_identifier");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("cocoa", (yyvsp[0].id));
      }
    }
#line 1754 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 20:
#line 385 "thrifty.yy" /* yacc.c:1646  */
    {
      pwarning(1, "'xsd_namespace' is deprecated. Use 'namespace xsd' instead");
      pdebug("Header -> tok_xsd_namespace tok_literal");
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("cocoa", (yyvsp[0].id));
      }
    }
#line 1766 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 21:
#line 394 "thrifty.yy" /* yacc.c:1646  */
    {
     pwarning(1, "'csharp_namespace' is deprecated. Use 'namespace csharp' instead");
     pdebug("Header -> tok_csharp_namespace tok_identifier");
     if (g_parse_mode == PROGRAM) {
       g_program->set_namespace("csharp", (yyvsp[0].id));
     }
   }
#line 1778 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 22:
#line 403 "thrifty.yy" /* yacc.c:1646  */
    {
     pwarning(1, "'delphi_namespace' is deprecated. Use 'namespace delphi' instead");
     pdebug("Header -> tok_delphi_namespace tok_identifier");
     if (g_parse_mode == PROGRAM) {
       g_program->set_namespace("delphi", (yyvsp[0].id));
     }
   }
#line 1790 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 23:
#line 413 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Include -> tok_include tok_literal");
      if (g_parse_mode == INCLUDES) {
        std::string path = include_file(std::string((yyvsp[0].id)));
        if (!path.empty()) {
          g_program->add_include(path, std::string((yyvsp[0].id)));
        }
      }
    }
#line 1804 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 24:
#line 425 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("DefinitionList -> DefinitionList Definition");
      if ((yyvsp[-1].dtext) != NULL && (yyvsp[0].tdoc) != NULL) {
        (yyvsp[0].tdoc)->set_doc((yyvsp[-1].dtext));
      }
    }
#line 1815 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 25:
#line 432 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("DefinitionList -> ");
    }
#line 1823 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 26:
#line 438 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Definition -> Const");
      if (g_parse_mode == PROGRAM) {
        g_program->add_const((yyvsp[0].tconst));
      }
      (yyval.tdoc) = (yyvsp[0].tconst);
    }
#line 1835 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 27:
#line 446 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Definition -> TypeDefinition");
      if (g_parse_mode == PROGRAM) {
        g_scope->add_type((yyvsp[0].ttype)->get_name(), (yyvsp[0].ttype));
        if (g_parent_scope != NULL) {
          g_parent_scope->add_type(g_parent_prefix + (yyvsp[0].ttype)->get_name(), (yyvsp[0].ttype));
        }
      }
      (yyval.tdoc) = (yyvsp[0].ttype);
    }
#line 1850 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 28:
#line 457 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Definition -> Service");
      if (g_parse_mode == PROGRAM) {
        g_scope->add_service((yyvsp[0].tservice)->get_name(), (yyvsp[0].tservice));
        if (g_parent_scope != NULL) {
          g_parent_scope->add_service(g_parent_prefix + (yyvsp[0].tservice)->get_name(), (yyvsp[0].tservice));
        }
        g_program->add_service((yyvsp[0].tservice));
      }
      (yyval.tdoc) = (yyvsp[0].tservice);
    }
#line 1866 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 29:
#line 471 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeDefinition -> Typedef");
      if (g_parse_mode == PROGRAM) {
        g_program->add_typedef((yyvsp[0].ttypedef));
      }
    }
#line 1877 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 30:
#line 478 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeDefinition -> Enum");
      if (g_parse_mode == PROGRAM) {
        g_program->add_enum((yyvsp[0].tenum));
      }
    }
#line 1888 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 31:
#line 485 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeDefinition -> Senum");
      if (g_parse_mode == PROGRAM) {
        g_program->add_typedef((yyvsp[0].ttypedef));
      }
    }
#line 1899 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 32:
#line 492 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeDefinition -> Struct");
      if (g_parse_mode == PROGRAM) {
        g_program->add_struct((yyvsp[0].tstruct));
      }
    }
#line 1910 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 33:
#line 499 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeDefinition -> Xception");
      if (g_parse_mode == PROGRAM) {
        g_program->add_xception((yyvsp[0].tstruct));
      }
    }
#line 1921 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 34:
#line 508 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeDef -> tok_typedef FieldType tok_identifier");
      t_typedef *td = new t_typedef(g_program, (yyvsp[-2].ttype), (yyvsp[-1].id));
      (yyval.ttypedef) = td;
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.ttypedef)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 1935 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 35:
#line 520 "thrifty.yy" /* yacc.c:1646  */
    {}
#line 1941 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 36:
#line 522 "thrifty.yy" /* yacc.c:1646  */
    {}
#line 1947 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 37:
#line 524 "thrifty.yy" /* yacc.c:1646  */
    {}
#line 1953 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 38:
#line 528 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Enum -> tok_enum tok_identifier { EnumDefList }");
      (yyval.tenum) = (yyvsp[-2].tenum);
      (yyval.tenum)->set_name((yyvsp[-4].id));
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.tenum)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
      (yyval.tenum)->resolve_values();
      // make constants for all the enum values
      if (g_parse_mode == PROGRAM) {
        const std::vector<t_enum_value*>& enum_values = (yyval.tenum)->get_constants();
        std::vector<t_enum_value*>::const_iterator c_iter;
        for (c_iter = enum_values.begin(); c_iter != enum_values.end(); ++c_iter) {
          std::string const_name = (yyval.tenum)->get_name() + "." + (*c_iter)->get_name();
          t_const_value* const_val = new t_const_value((*c_iter)->get_value());
          const_val->set_enum((yyval.tenum));
          g_scope->add_constant(const_name, new t_const(g_type_i32, (*c_iter)->get_name(), const_val));
          if (g_parent_scope != NULL) {
            g_parent_scope->add_constant(g_parent_prefix + const_name, new t_const(g_type_i32, (*c_iter)->get_name(), const_val));
          }
        }
      }
    }
#line 1982 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 39:
#line 555 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("EnumDefList -> EnumDefList EnumDef");
      (yyval.tenum) = (yyvsp[-1].tenum);
      (yyval.tenum)->append((yyvsp[0].tenumv));
    }
#line 1992 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 40:
#line 561 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("EnumDefList -> ");
      (yyval.tenum) = new t_enum(g_program);
    }
#line 2001 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 41:
#line 568 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("EnumDef -> tok_identifier = tok_int_constant");
      if ((yyvsp[-2].iconst) < 0) {
        pwarning(1, "Negative value supplied for enum %s.\n", (yyvsp[-4].id));
      }
      if ((yyvsp[-2].iconst) > INT_MAX) {
        pwarning(1, "64-bit value supplied for enum %s.\n", (yyvsp[-4].id));
      }
      (yyval.tenumv) = new t_enum_value((yyvsp[-4].id), (yyvsp[-2].iconst));
      if ((yyvsp[-5].dtext) != NULL) {
        (yyval.tenumv)->set_doc((yyvsp[-5].dtext));
      }
      if ((yyvsp[-1].ttype) != NULL) {
        (yyval.tenumv)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 2023 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 42:
#line 587 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("EnumDef -> tok_identifier");
      (yyval.tenumv) = new t_enum_value((yyvsp[-2].id));
      if ((yyvsp[-3].dtext) != NULL) {
        (yyval.tenumv)->set_doc((yyvsp[-3].dtext));
      }
      if ((yyvsp[-1].ttype) != NULL) {
        (yyval.tenumv)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 2039 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 43:
#line 601 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Senum -> tok_senum tok_identifier { SenumDefList }");
      (yyval.ttypedef) = new t_typedef(g_program, (yyvsp[-2].tbase), (yyvsp[-4].id));
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.ttypedef)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2052 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 44:
#line 612 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SenumDefList -> SenumDefList SenumDef");
      (yyval.tbase) = (yyvsp[-1].tbase);
      (yyval.tbase)->add_string_enum_val((yyvsp[0].id));
    }
#line 2062 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 45:
#line 618 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SenumDefList -> ");
      (yyval.tbase) = new t_base_type("string", t_base_type::TYPE_STRING);
      (yyval.tbase)->set_string_enum(true);
    }
#line 2072 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 46:
#line 626 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SenumDef -> tok_literal");
      (yyval.id) = (yyvsp[-1].id);
    }
#line 2081 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 47:
#line 633 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Const -> tok_const FieldType tok_identifier = ConstValue");
      if (g_parse_mode == PROGRAM) {
        g_scope->resolve_const_value((yyvsp[-1].tconstv), (yyvsp[-4].ttype));
        (yyval.tconst) = new t_const((yyvsp[-4].ttype), (yyvsp[-3].id), (yyvsp[-1].tconstv));
        validate_const_type((yyval.tconst));

        g_scope->add_constant((yyvsp[-3].id), (yyval.tconst));
        if (g_parent_scope != NULL) {
          g_parent_scope->add_constant(g_parent_prefix + (yyvsp[-3].id), (yyval.tconst));
        }
      } else {
        (yyval.tconst) = NULL;
      }
    }
#line 2101 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 48:
#line 651 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstValue => tok_int_constant");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_integer((yyvsp[0].iconst));
      if (!g_allow_64bit_consts && ((yyvsp[0].iconst) < INT32_MIN || (yyvsp[0].iconst) > INT32_MAX)) {
        pwarning(1, "64-bit constant \"%"PRIi64"\" may not work in all languages.\n", (yyvsp[0].iconst));
      }
    }
#line 2114 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 49:
#line 660 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstValue => tok_dub_constant");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_double((yyvsp[0].dconst));
    }
#line 2124 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 50:
#line 666 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstValue => tok_literal");
      (yyval.tconstv) = new t_const_value((yyvsp[0].id));
    }
#line 2133 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 51:
#line 671 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstValue => tok_identifier");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_identifier((yyvsp[0].id));
    }
#line 2143 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 52:
#line 677 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstValue => ConstList");
      (yyval.tconstv) = (yyvsp[0].tconstv);
    }
#line 2152 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 53:
#line 682 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstValue => ConstMap");
      (yyval.tconstv) = (yyvsp[0].tconstv);
    }
#line 2161 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 54:
#line 689 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstList => [ ConstListContents ]");
      (yyval.tconstv) = (yyvsp[-1].tconstv);
    }
#line 2170 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 55:
#line 696 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstListContents => ConstListContents ConstValue CommaOrSemicolonOptional");
      (yyval.tconstv) = (yyvsp[-2].tconstv);
      (yyval.tconstv)->add_list((yyvsp[-1].tconstv));
    }
#line 2180 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 56:
#line 702 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstListContents =>");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_list();
    }
#line 2190 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 57:
#line 710 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstMap => { ConstMapContents }");
      (yyval.tconstv) = (yyvsp[-1].tconstv);
    }
#line 2199 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 58:
#line 717 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstMapContents => ConstMapContents ConstValue CommaOrSemicolonOptional");
      (yyval.tconstv) = (yyvsp[-4].tconstv);
      (yyval.tconstv)->add_map((yyvsp[-3].tconstv), (yyvsp[-1].tconstv));
    }
#line 2209 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 59:
#line 723 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ConstMapContents =>");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_map();
    }
#line 2219 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 60:
#line 731 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.iconst) = struct_is_struct;
    }
#line 2227 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 61:
#line 735 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.iconst) = struct_is_union;
    }
#line 2235 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 62:
#line 741 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Struct -> tok_struct tok_identifier { FieldList }");
      (yyvsp[-2].tstruct)->set_xsd_all((yyvsp[-4].tbool));
      (yyvsp[-2].tstruct)->set_union((yyvsp[-6].iconst) == struct_is_union);
      (yyval.tstruct) = (yyvsp[-2].tstruct);
      (yyval.tstruct)->set_name((yyvsp[-5].id));
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.tstruct)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
      if ((yyvsp[-6].iconst) == struct_is_union) {
        validate_union((yyval.tstruct));
      }
    }
#line 2254 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 63:
#line 758 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = true;
    }
#line 2262 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 64:
#line 762 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = false;
    }
#line 2270 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 65:
#line 768 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = true;
    }
#line 2278 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 66:
#line 772 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = false;
    }
#line 2286 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 67:
#line 778 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = true;
    }
#line 2294 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 68:
#line 782 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = false;
    }
#line 2302 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 69:
#line 788 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tstruct) = (yyvsp[-1].tstruct);
    }
#line 2310 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 70:
#line 792 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tstruct) = NULL;
    }
#line 2318 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 71:
#line 798 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Xception -> tok_xception tok_identifier { FieldList }");
      (yyvsp[-2].tstruct)->set_name((yyvsp[-4].id));
      (yyvsp[-2].tstruct)->set_xception(true);
      (yyval.tstruct) = (yyvsp[-2].tstruct);
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.tstruct)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2333 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 72:
#line 811 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Service -> tok_service tok_identifier { FunctionList }");
      (yyval.tservice) = (yyvsp[-3].tservice);
      (yyval.tservice)->set_name((yyvsp[-7].id));
      (yyval.tservice)->set_extends((yyvsp[-6].tservice));
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.tservice)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2348 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 73:
#line 823 "thrifty.yy" /* yacc.c:1646  */
    {
       g_arglist = 1;
    }
#line 2356 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 74:
#line 828 "thrifty.yy" /* yacc.c:1646  */
    {
       g_arglist = 0;
    }
#line 2364 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 75:
#line 834 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Extends -> tok_extends tok_identifier");
      (yyval.tservice) = NULL;
      if (g_parse_mode == PROGRAM) {
        (yyval.tservice) = g_scope->get_service((yyvsp[0].id));
        if ((yyval.tservice) == NULL) {
          yyerror("Service \"%s\" has not been defined.", (yyvsp[0].id));
          exit(1);
        }
      }
    }
#line 2380 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 76:
#line 846 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tservice) = NULL;
    }
#line 2388 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 77:
#line 852 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FunctionList -> FunctionList Function");
      (yyval.tservice) = (yyvsp[-1].tservice);
      (yyvsp[-1].tservice)->add_function((yyvsp[0].tfunction));
    }
#line 2398 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 78:
#line 858 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FunctionList -> ");
      (yyval.tservice) = new t_service(g_program);
    }
#line 2407 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 79:
#line 865 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyvsp[-4].tstruct)->set_name(std::string((yyvsp[-6].id)) + "_args");
      (yyval.tfunction) = new t_function((yyvsp[-7].ttype), (yyvsp[-6].id), (yyvsp[-4].tstruct), (yyvsp[-2].tstruct), (yyvsp[-8].tbool));
      if ((yyvsp[-9].dtext) != NULL) {
        (yyval.tfunction)->set_doc((yyvsp[-9].dtext));
      }
      if ((yyvsp[-1].ttype) != NULL) {
        (yyval.tfunction)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 2423 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 80:
#line 879 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = true;
    }
#line 2431 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 81:
#line 883 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tbool) = false;
    }
#line 2439 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 82:
#line 889 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("Throws -> tok_throws ( FieldList )");
      (yyval.tstruct) = (yyvsp[-1].tstruct);
      if (g_parse_mode == PROGRAM && !validate_throws((yyval.tstruct))) {
        yyerror("Throws clause may not contain non-exception types");
        exit(1);
      }
    }
#line 2452 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 83:
#line 898 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tstruct) = new t_struct(g_program);
    }
#line 2460 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 84:
#line 904 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FieldList -> FieldList , Field");
      (yyval.tstruct) = (yyvsp[-1].tstruct);
      if (!((yyval.tstruct)->append((yyvsp[0].tfield)))) {
        yyerror("Field identifier %d for \"%s\" has already been used", (yyvsp[0].tfield)->get_key(), (yyvsp[0].tfield)->get_name().c_str());
        exit(1);
      }
    }
#line 2473 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 85:
#line 913 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FieldList -> ");
      y_field_val = -1;
      (yyval.tstruct) = new t_struct(g_program);
    }
#line 2483 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 86:
#line 921 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("tok_int_constant : Field -> FieldType tok_identifier");
      if ((yyvsp[-9].tfieldid).auto_assigned) {
        pwarning(1, "No field key specified for %s, resulting protocol may have conflicts or not be backwards compatible!\n", (yyvsp[-6].id));
        if (g_strict >= 192) {
          yyerror("Implicit field keys are deprecated and not allowed with -strict");
          exit(1);
        }
      }
      (yyval.tfield) = new t_field((yyvsp[-7].ttype), (yyvsp[-6].id), (yyvsp[-9].tfieldid).value);
      (yyval.tfield)->set_req((yyvsp[-8].ereq));
      if ((yyvsp[-5].tconstv) != NULL) {
        g_scope->resolve_const_value((yyvsp[-5].tconstv), (yyvsp[-7].ttype));
        validate_field_value((yyval.tfield), (yyvsp[-5].tconstv));
        (yyval.tfield)->set_value((yyvsp[-5].tconstv));
      }
      (yyval.tfield)->set_xsd_optional((yyvsp[-4].tbool));
      (yyval.tfield)->set_xsd_nillable((yyvsp[-3].tbool));
      if ((yyvsp[-10].dtext) != NULL) {
        (yyval.tfield)->set_doc((yyvsp[-10].dtext));
      }
      if ((yyvsp[-2].tstruct) != NULL) {
        (yyval.tfield)->set_xsd_attrs((yyvsp[-2].tstruct));
      }
      if ((yyvsp[-1].ttype) != NULL) {
        (yyval.tfield)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 2517 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 87:
#line 953 "thrifty.yy" /* yacc.c:1646  */
    {
      if ((yyvsp[-1].iconst) <= 0) {
        if (g_allow_neg_field_keys) {
          /*
           * g_allow_neg_field_keys exists to allow users to add explicitly
           * specified key values to old .thrift files without breaking
           * protocol compatibility.
           */
          if ((yyvsp[-1].iconst) != y_field_val) {
            /*
             * warn if the user-specified negative value isn't what
             * thrift would have auto-assigned.
             */
            pwarning(1, "Negative field key (%d) differs from what would be "
                     "auto-assigned by thrift (%d).\n", (yyvsp[-1].iconst), y_field_val);
          }
          /*
           * Leave $1 as-is, and update y_field_val to be one less than $1.
           * The FieldList parsing will catch any duplicate key values.
           */
          y_field_val = (yyvsp[-1].iconst) - 1;
          (yyval.tfieldid).value = (yyvsp[-1].iconst);
          (yyval.tfieldid).auto_assigned = false;
        } else {
          pwarning(1, "Nonpositive value (%d) not allowed as a field key.\n",
                   (yyvsp[-1].iconst));
          (yyval.tfieldid).value = y_field_val--;
          (yyval.tfieldid).auto_assigned = true;
        }
      } else {
        (yyval.tfieldid).value = (yyvsp[-1].iconst);
        (yyval.tfieldid).auto_assigned = false;
      }
    }
#line 2556 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 88:
#line 988 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tfieldid).value = y_field_val--;
      (yyval.tfieldid).auto_assigned = true;
    }
#line 2565 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 89:
#line 995 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.ereq) = t_field::T_REQUIRED;
    }
#line 2573 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 90:
#line 999 "thrifty.yy" /* yacc.c:1646  */
    {
      if (g_arglist) {
        if (g_parse_mode == PROGRAM) {
          pwarning(1, "optional keyword is ignored in argument lists.\n");
        }
        (yyval.ereq) = t_field::T_OPT_IN_REQ_OUT;
      } else {
        (yyval.ereq) = t_field::T_OPTIONAL;
      }
    }
#line 2588 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 91:
#line 1010 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.ereq) = t_field::T_OPT_IN_REQ_OUT;
    }
#line 2596 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 92:
#line 1016 "thrifty.yy" /* yacc.c:1646  */
    {
      if (g_parse_mode == PROGRAM) {
        (yyval.tconstv) = (yyvsp[0].tconstv);
      } else {
        (yyval.tconstv) = NULL;
      }
    }
#line 2608 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 93:
#line 1024 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.tconstv) = NULL;
    }
#line 2616 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 94:
#line 1030 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FunctionType -> FieldType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2625 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 95:
#line 1035 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FunctionType -> tok_void");
      (yyval.ttype) = g_type_void;
    }
#line 2634 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 96:
#line 1042 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FieldType -> tok_identifier");
      if (g_parse_mode == INCLUDES) {
        // Ignore identifiers in include mode
        (yyval.ttype) = NULL;
      } else {
        // Lookup the identifier in the current scope
        (yyval.ttype) = g_scope->get_type((yyvsp[0].id));
        if ((yyval.ttype) == NULL) {
          yyerror("Type \"%s\" has not been defined.", (yyvsp[0].id));
          exit(1);
        }
      }
    }
#line 2653 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 97:
#line 1057 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FieldType -> BaseType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2662 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 98:
#line 1062 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("FieldType -> ContainerType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2671 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 99:
#line 1068 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> SimpleBaseType TypeAnnotations");
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.ttype) = new t_base_type(*static_cast<t_base_type*>((yyvsp[-1].ttype)));
        (yyval.ttype)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      } else {
        (yyval.ttype) = (yyvsp[-1].ttype);
      }
    }
#line 2686 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 100:
#line 1081 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_string");
      (yyval.ttype) = g_type_string;
    }
#line 2695 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 101:
#line 1086 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_binary");
      (yyval.ttype) = g_type_binary;
    }
#line 2704 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 102:
#line 1091 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_slist");
      (yyval.ttype) = g_type_slist;
    }
#line 2713 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 103:
#line 1096 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_bool");
      (yyval.ttype) = g_type_bool;
    }
#line 2722 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 104:
#line 1101 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_byte");
      (yyval.ttype) = g_type_byte;
    }
#line 2731 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 105:
#line 1106 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_i16");
      (yyval.ttype) = g_type_i16;
    }
#line 2740 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 106:
#line 1111 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_i32");
      (yyval.ttype) = g_type_i32;
    }
#line 2749 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 107:
#line 1116 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_i64");
      (yyval.ttype) = g_type_i64;
    }
#line 2758 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 108:
#line 1121 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("BaseType -> tok_double");
      (yyval.ttype) = g_type_double;
    }
#line 2767 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 109:
#line 1127 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ContainerType -> SimpleContainerType TypeAnnotations");
      (yyval.ttype) = (yyvsp[-1].ttype);
      if ((yyvsp[0].ttype) != NULL) {
        (yyval.ttype)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2780 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 110:
#line 1138 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SimpleContainerType -> MapType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2789 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 111:
#line 1143 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SimpleContainerType -> SetType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2798 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 112:
#line 1148 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SimpleContainerType -> ListType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2807 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 113:
#line 1155 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("MapType -> tok_map <FieldType, FieldType>");
      (yyval.ttype) = new t_map((yyvsp[-3].ttype), (yyvsp[-1].ttype));
      if ((yyvsp[-5].id) != NULL) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[-5].id)));
      }
    }
#line 2819 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 114:
#line 1165 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("SetType -> tok_set<FieldType>");
      (yyval.ttype) = new t_set((yyvsp[-1].ttype));
      if ((yyvsp[-3].id) != NULL) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[-3].id)));
      }
    }
#line 2831 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 115:
#line 1175 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("ListType -> tok_list<FieldType>");
      (yyval.ttype) = new t_list((yyvsp[-2].ttype));
      if ((yyvsp[0].id) != NULL) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[0].id)));
      }
    }
#line 2843 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 116:
#line 1185 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.id) = (yyvsp[0].id);
    }
#line 2851 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 117:
#line 1189 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.id) = NULL;
    }
#line 2859 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 118:
#line 1195 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeAnnotations -> ( TypeAnnotationList )");
      (yyval.ttype) = (yyvsp[-1].ttype);
    }
#line 2868 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 119:
#line 1200 "thrifty.yy" /* yacc.c:1646  */
    {
      (yyval.ttype) = NULL;
    }
#line 2876 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 120:
#line 1206 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeAnnotationList -> TypeAnnotationList , TypeAnnotation");
      (yyval.ttype) = (yyvsp[-1].ttype);
      (yyval.ttype)->annotations_[(yyvsp[0].tannot)->key] = (yyvsp[0].tannot)->val;
      delete (yyvsp[0].tannot);
    }
#line 2887 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 121:
#line 1213 "thrifty.yy" /* yacc.c:1646  */
    {
      /* Just use a dummy structure to hold the annotations. */
      (yyval.ttype) = new t_struct(g_program);
    }
#line 2896 "thrifty.cc" /* yacc.c:1646  */
    break;

  case 122:
#line 1220 "thrifty.yy" /* yacc.c:1646  */
    {
      pdebug("TypeAnnotation -> tok_identifier = tok_literal");
      (yyval.tannot) = new t_annotation;
      (yyval.tannot)->key = (yyvsp[-3].id);
      (yyval.tannot)->val = (yyvsp[-1].id);
    }
#line 2907 "thrifty.cc" /* yacc.c:1646  */
    break;


#line 2911 "thrifty.cc" /* yacc.c:1646  */
      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[*yyssp], yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 1227 "thrifty.yy" /* yacc.c:1906  */

