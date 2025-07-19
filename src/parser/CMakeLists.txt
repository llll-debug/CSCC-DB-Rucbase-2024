find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

set(LEXER_OUT "${CMAKE_CURRENT_SOURCE_DIR}/lexer.cpp")
set(LEXER_DEFINES "${CMAKE_CURRENT_SOURCE_DIR}/lexer.h")
set(PARSER_OUT "${CMAKE_CURRENT_SOURCE_DIR}/parser.yy.cpp")
set(PARSER_DEFINES "${CMAKE_CURRENT_SOURCE_DIR}/parser.yy.h")

flex_target(lex lex.l "${LEXER_OUT}" DEFINES_FILE "${LEXER_DEFINES}")
bison_target(yacc yacc.y "${PARSER_OUT}" DEFINES_FILE "${PARSER_DEFINES}")
add_flex_bison_dependency(lex yacc)

set(SOURCES ${BISON_yacc_OUTPUT_SOURCE} ${FLEX_lex_OUTPUTS} ast.cpp)
add_library(parser STATIC ${SOURCES})

add_executable(test_parser test_parser.cpp)
target_link_libraries(test_parser parser)
add_test(NAME test_parser COMMAND test_parser
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
