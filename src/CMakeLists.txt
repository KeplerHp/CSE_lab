# add_subdirectory(block)
# add_subdirectory(metadata)
# add_subdirectory(filesystem)
# add_subdirectory(librpc)
# add_subdirectory(distributed)
add_subdirectory(map_reduce)


add_library(bins1 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins1 PROPERTY IMPORTED_OBJECTS allocator.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins1>)

add_library(bins2 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins2 PROPERTY IMPORTED_OBJECTS chfs_block.dir.manager.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins2>)

add_library(bins3 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins3 PROPERTY IMPORTED_OBJECTS chfs_distributed.dir.client.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins3>)

add_library(bins4 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins4 PROPERTY IMPORTED_OBJECTS commit_log.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins4>)

add_library(bins5 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins5 PROPERTY IMPORTED_OBJECTS dataserver.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins5>)

add_library(bins6 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins6 PROPERTY IMPORTED_OBJECTS metadata_server.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins6>)

add_library(bins7 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins7 PROPERTY IMPORTED_OBJECTS control_op.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins7>)

add_library(bins8 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins8 PROPERTY IMPORTED_OBJECTS data_op.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins8>)

add_library(bins9 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins9 PROPERTY IMPORTED_OBJECTS directory_op.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins9>)

add_library(bins10 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins10 PROPERTY IMPORTED_OBJECTS chfs_rpc.dir.client.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins10>)

add_library(bins11 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins11 PROPERTY IMPORTED_OBJECTS server.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins11>)

add_library(bins12 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins12 PROPERTY IMPORTED_OBJECTS inode.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins12>)

add_library(bins13 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins13 PROPERTY IMPORTED_OBJECTS chfs_metadata.dir.manager.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins13>)

add_library(bins14 OBJECT IMPORTED GLOBAL)
set_property(TARGET bins14 PROPERTY IMPORTED_OBJECTS superblock.cc.o)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bins14>)

add_library(chfs STATIC ${ALL_OBJECT_FILES})

set(CHFS_THIRDPARTY_LIBS
        fmt
        rpc
        )

target_link_libraries(
        chfs
        ${CHFS_THIRDPARTY_LIBS})
