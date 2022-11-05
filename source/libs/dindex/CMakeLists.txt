aux_source_directory(src DINDEX_SRC)
add_library(wal STATIC ${DINDEX_SRC})
target_include_directories(
    wal
    PUBLIC "${TD_SOURCE_DIR}/include/libs/dindex"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    dindex
    PUBLIC cjson
    PUBLIC os
    PUBLIC util
    PUBLIC common
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
