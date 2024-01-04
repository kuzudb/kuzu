import os

# Change the current working directory
os.system("pip3 install rangehttpserver")
os.chdir(f'{os.getcwd()}/../dataset')
os.system("python3 -m RangeHTTPServer 80")
