def clobber_calcos(func):
    '''
    This is a decorator to be used to clobber output files from calcos.
    If the output products already exist, they will be deleted and calcos
    re-run for the particular infile.

    Use:
    ----
        This should be imported and used as an excplicit decorator:

        import calcos
        from dec_calcos import clobber_calcos
        new_calcos = clobber_calcos(calcos.calcos)
        new_calcos(input...)

    Parameters:
    -----------
        func : function
            The input function to decorate.

    Returns:
    --------
        wrapper : function
            A wrapper to the modified function.
    '''

    __author__ = "Jo Taylor"
    __date__ = "02-25-2016"
    __maintainer__ = "Jo Taylor"
    __email__ = "jotaylor@stsci.edu"

    import os
    import glob
    from astropy.io import fits

    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            # If the reason for crashing is that the output files already exist
            if type(e).__name__ == "RuntimeError" and \
               e.args[0] == "output files already exist":
                # If asntable defined in kwargs or just in args
                if "asntable" in kwargs.keys():
                    asntable = kwargs["asntable"]
                else:
                    asntable = args[0]
                filename = os.path.basename(asntable)
                if "outdir" in kwargs.keys():
                    outdir = kwargs["outdir"]
                elif len(args) >= 2:
                    outdir = args[1]
                else:
                    outdir = None
                # If the asntable is an association file, not rawtag,
                # get the member IDs
                if "asn.fits" in filename:
                    with fits.open(asntable) as hdu:
                        data = hdu[1].data
                        memnames = data["memname"]
                        f_to_remove = [root.lower() for root in memnames]
                    # If an output directory is specified, the asn file is
                    # is copied to there and must be removed as well.
                    if outdir:
                       os.remove(os.path.join(outdir, filename))
                # if rawtag, just get the rawtag rootname to delete
                else:
                    f_to_remove = [filename[:9]]
                # If no outdir specified, it is the current directory
                if not outdir:
                    outdir = "."
                for item in f_to_remove:
                    matching = glob.glob(os.path.join(outdir,item+"*fits*"))
                    for match in matching:
                        ext = match.split("/")[-1].split("_")[1].split(".")[0]
                        if ext not in ["asn", "pha", "rawaccum", "rawacq",
                                       "rawtag", "spt"]:
                            os.remove(match)
                print("="*72 + "\n" + "="*72)
                print("!!!WARNING!!! Deleting products for {} !!!".format(
                      asntable))
                print("CalCOS will now calibrate {}...".format(asntable))
                print("="*72 + "\n" + "="*72)
                func(*args, **kwargs)
            else:
                raise e
    return wrapper
