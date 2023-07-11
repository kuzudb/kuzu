"""
Patches arrow to pass the information about how to find curl to google_cloud_cpp
Wrapped in a python launcher so that we can ignore failures 
(CMake may run it on non-fresh repositories, and the patch will fail to apply)
"""

patch = """--- a/cpp/cmake_modules/ThirdpartyToolchain.cmake	2023-07-10 14:14:24.829431982 -0400
+++ b/cpp/cmake_modules/ThirdpartyToolchain.cmake	2023-07-11 16:49:13.813712785 -0400
@@ -4239,7 +4239,9 @@
       -DGOOGLE_CLOUD_CPP_ENABLE_WERROR=OFF
       -DOPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
       -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
-      -DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY})
+      -DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY}
+      -DCURL_INCLUDE_DIR=${CURL_INCLUDE_DIR}
+      -DCURL_LIBRARY=${CURL_LIBRARY})
 
   add_custom_target(google_cloud_cpp_dependencies)

"""

from subprocess import Popen, PIPE

proc = Popen(["git", "apply", "-"], stdin=PIPE)
proc.communicate(input=patch.encode("utf-8"))
