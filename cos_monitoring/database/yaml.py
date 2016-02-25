from __future__ import print_function, absolute_import, division

#-------------------------------------------------------------------------------

class yaml:
    """Simple YAML parser with very limited functionality

    SHOULD BE REMOVED ONCE SSO ISSUES ARE DEALT-WITH

    """
    @staticmethod
    def load(filename):
        out_info = {}

        for line in open(filename).readlines():
            line = line.split(':')

            key = line[0].strip()
            val = line[1].strip()

            out_info[key] = val

        return out_info

#-------------------------------------------------------------------------------
