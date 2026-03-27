add_executable(test_c_api test_c_api.cpp)
target_link_libraries(test_c_api PRIVATE dingofs_connector pthread)
target_include_directories(test_c_api PRIVATE ${CMAKE_SOURCE_DIR}/src/io_engine)

add_test(NAME test_c_api COMMAND test_c_api)
