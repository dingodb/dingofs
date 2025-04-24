#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_int32(v);

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  FLAGS_v = 10;
  return RUN_ALL_TESTS();
}
