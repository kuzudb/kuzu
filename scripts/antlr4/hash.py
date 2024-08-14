import hashlib, sys
print(hashlib.md5((''.join(sys.stdin)).encode('utf-8')).hexdigest())
