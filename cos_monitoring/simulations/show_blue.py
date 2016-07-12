import matplotlib.pyplot as plt
import pyfits
import numpy as np

#plt.ioff()
plt.ion()

gsagtab = pyfits.open('/grp/hst/cdbs/lref/x6l1439el_gsag.fits')
xtractab = pyfits.open('/grp/hst/cdbs/lref/x6q17586l_1dx.fits')
gainmap = pyfits.open('/grp/hst/cos/coslife/gainmaps/total_gain_163.fits')

def show( segment='FUVB' ):
    fig = plt.figure( figsize=(24,12) )
    ax = fig.add_subplot( 1,1,1 )

    if segment == 'FUVA':
        ext = 1
        gsag_ext = 24
    elif segment == 'FUVB':
        ext = 2
        gsag_ext = 63

    ax.imshow( gainmap[ext].data, aspect='auto', interpolation='nearest', vmin=0, vmax=20, cmap=plt.get_cmap('Greys') )
    
    for i, row in enumerate( gsagtab[gsag_ext].data ):
        lx = row['lx']
        ly = row['ly']
        dx = row['dx']
        dy = row['dy']
        if not i:
            ax.plot( [lx, lx+dx, lx+dx, lx, lx], [ly, ly, ly+dy, ly+dy, ly], color='r', label='Flagged Low Gain' )
        else:
            ax.plot( [lx, lx+dx, lx+dx, lx, lx], [ly, ly, ly+dy, ly+dy, ly], color='r')

    index = np.where( (xtractab[1].data['SEGMENT'] == segment) &
                      ( (xtractab[1].data['CENWAVE'] == 1055) |
                        (xtractab[1].data['CENWAVE'] == 1096) ) &
                      (xtractab[1].data['APERTURE'] == 'PSA') 
                      )[0]

    color_dict = { 1055:'blue',
                   1096:'cyan'
                   }
    style_dict = {1055:'-',
                  1096:'--'
                  }

    for row in xtractab[1].data[ index ]:
        #loc = row['B_SPEC'] - 82
        loc = row['B_SPEC']
        height = row['HEIGHT']
        cenwave = row['cenwave']

        ax.axhline( y= (loc-height/2), lw=5, ls=style_dict[cenwave], 
                    color=color_dict[cenwave], label='{}'.format( cenwave ) )
        ax.axhline( y= (loc+height/2), lw=5, ls=style_dict[cenwave], 
                    color=color_dict[cenwave])

    ax.set_xlim(0, 16384)
    ax.set_ylim(300, 800)

    ax.legend( shadow=True, numpoints=1 )
    ax.set_xlabel('XCORR')
    ax.set_ylabel('YCORR')

    ax.set_title('%s at HV=163 and LP2' % segment)

    raw_input()

    fig.savefig( 'LP2_%s_blue_extraction.pdf' % (segment), bbox_inches='tight' )
    
if __name__ == "__main__":
    show('FUVA')
    show('FUVB')
