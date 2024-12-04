# `pip install` from the source
```
chmod +x package_tar.py
./package_tar.py
pip install kuzu.tar.gz    
```

Note: installing from source requires the full toolchain for building the project, including Cmake(>=3.11), Python 3, and a compiler compatible with C++20. The package works for both Linux and macOS.
