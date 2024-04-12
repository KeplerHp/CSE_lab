add_subdirectory(block)
add_subdirectory(metadata)
add_subdirectory(filesystem)
add_subdirectory(librpc)
add_subdirectory(distributed)

add_library(chfs_block_manager OBJECT IMPORTED GLOBAL)
set_property(TARGET chfs_block_manager PROPERTY IMPORTED_OBJECTS manager.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:chfs_block_manager>)

add_library(chfs STATIC ${ALL_OBJECT_FILES})

set(CHFS_THIRDPARTY_LIBS
        fmt
        rpc
        )

target_link_libraries(
        chfs
        ${CHFS_THIRDPARTY_LIBS})
