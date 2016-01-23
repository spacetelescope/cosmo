// Read in CCI file data extensions in C++
// g++ program.c -I/grp/software/Linux/RH5/x86_64/cfitsio/include -L/grp/software/Linux/RH5/x86_64/cfitsio/lib -lcfitsio -lm

# include <Python.h>
# include <numpy/arrayobject.h>
# include "fitsio.h"
# include <stdlib.h>

# define NZ 32
# define NY 1024
# define NX 16384

static PyObject *cci_read(PyObject *self, PyObject *args) {

  /* local variables */
  int i=0,j=0,k=0,n=0;
  long fpixel[2]= {1,1};
  long numPixels = 1024 * 16384;
  int axis_lengths[3] = {NZ,NY,NX};
  fitsfile *fitsFilePtr;         
  int status = 0; /* Must initialize status before use */
  float *image = (float*)malloc((NY * NX) * sizeof(float));

  const char *filename;
  if (!PyArg_ParseTuple(args, "s", &filename) ) {
    return NULL;
   }

  /* returned array */
  npy_intp dims[1] = {NZ*NX*NY};
  PyArrayObject *out_array;
  out_array = (PyArrayObject *) PyArray_SimpleNew(1, dims, NPY_FLOAT);

  /* open the fits file */
  printf("Reading file: %s in c++\n",filename);
  fits_open_file(&fitsFilePtr, filename, READONLY, &status);
  fits_report_error(stdout, status);  

  for (k=3;k<=34;k++){
    /* Move through each extension and read the image array */
    fits_movabs_hdu(fitsFilePtr, k, NULL, &status);
    fits_report_error(stdout, status);  
    fits_read_pix(fitsFilePtr, TFLOAT, fpixel, numPixels, NULL, image, NULL, &status);
    fits_report_error(stdout, status);  
 
    for (j=0;j<NY; j++){
      for (i=0;i<NX; i++){
	/* Assign each pixel value directly to the output python array object */
	*(npy_float32 *) PyArray_GETPTR1(out_array, ( (k-3)*NY*NX) + (j*NX) + i) = (npy_float32) image[j * NX + i];
      }
    }
    
  }
  
  fits_close_file(fitsFilePtr, &status);
  free(image);

  
  return Py_BuildValue("N", out_array);
}

static PyMethodDef cci_read_methods[] = {
	{"read", cci_read, METH_VARARGS,
	"read cci data into a 3d array"},

	{NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initcci_read(void) {

	PyObject *mod;		/* the module */
	PyObject *dict;		/* the module's dictionary */

	mod = Py_InitModule("cci_read", cci_read_methods);
	import_array();

	/* set the doc string */
	dict = PyModule_GetDict(mod);
	//PyDict_SetItemString(dict, "__doc__",
	//	PyString_FromString(DocString()));
}
