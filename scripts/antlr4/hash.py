import hashlib, sys
print(hashlib.md5((''.join(open(sys.argv[1])) + ''.join(open(sys.argv[2]))).encode('utf-8')).hexdigest())
