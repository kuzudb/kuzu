file(REMOVE_RECURSE
  "libkuzu.a"
  "libkuzu.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/kuzu.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
