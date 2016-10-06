import matplotlib.pyplot as plt
import pyfits
import glob
import numpy as np

plt.ioff()
for item in glob.glob('simulation*.fits'):
    print(item)
    hdu = pyfits.open( item )
    gainmap = hdu[1].data

    plt.figure( figsize=(12,8) )

    #np.where( gainmap <=0, np.nan, gainmap )
    gainmap[500:577,2000:14000] = np.where( gainmap[500:577,2000:14000] <= 0, .5, gainmap[500:577,2000:14000] )
    #plt.imshow( gainmap, aspect='auto', interpolation='nearest' )
    #plt.clim(0,15)
    #plt.colorbar()
    plt.contourf( gainmap, levels=[0, 3, 5, 7, 9, 11, 15, 25],
                  colors=['Navy', 'CornflowerBlue', 'Orange', 'Yellow', 'Gold', 'Crimson', 'FireBrick', 'DarkRed'] )
    #if 'FUVA' in item:
    #    plt.xlim(500, 16000)
    #    plt.ylim(400, 600)
    #elif 'FUVB' in item:
    plt.xlim(500, 16000)
    plt.ylim(450, 650)

    plt.colorbar()
    plt.savefig( item.replace('.fits', '.png'), bbox_inches='tight'  )
