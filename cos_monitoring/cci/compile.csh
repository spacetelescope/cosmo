#!/bin/tcsh
gcc -fno-strict-aliasing -I/user/ely/cfitsio/include -I/usr/stsci/pyssgx/Python-2.7.3/include -I/usr/stsci/pyssgx/Python-2.7.3/include -DNDEBUG -I/usr/stsci/pyssgx/Python-2.7.3/include -I/usr/stsci/pyssgx/Python-2.7.3/include -fPIC -DNUMPY=1 -I/usr/stsci/pyssgx/Python-2.7.3/include/python2.7 -I/usr/stsci/pyssgx/Python-2.7.3/lib/python2.7/site-packages/numpy/core/include -I/usr/stsci/pyssgx/Python-2.7.3/include/python2.7 -c cci_read.c -o cci_read.o

cc -L/user/ely/cfitsio/lib -I/usr/stsci/pyssgx/Python-2.7.3/include -L/usr/stsci/pyssgx/Python-2.7.3/lib -pthread -shared -L/usr/stsci/pyssgx/Python-2.7.3/lib cci_read.o  -lcfitsio -lm -o cci_read.so
