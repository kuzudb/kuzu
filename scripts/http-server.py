import os

# Change the current working directory
os.chdir(f'{os.getcwd()}/../dataset')
os.system("python3 -m RangeHTTPServer 80")
