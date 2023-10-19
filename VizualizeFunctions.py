
import os
import glob
import pandas as pd
import numpy as np
import pycytominer
import string
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.pyplot as plt

def createwells(x):
    row384 = list(string.ascii_uppercase[:16])
    col384 = [(f'{i:02d}') for i in range(1, 25, 1)]
    wells384 = []

    for r in row384:
        for c in col384:
            wells384.append(str(r+c))
    return(wells384 * x)

def con_plates(df, featurename):
    df = df[["Metadata_Barcode","Metadata_Well", str(featurename)]]
    batch = df['Metadata_Barcode'].unique().tolist()
    
    ncols = 2
    nrows = len(batch) // ncols + (len(batch) % ncols > 0)
    
    vmax = df[str(featurename)].max()
    
    fig, axes = plt.subplots(nrows, ncols, figsize=(ncols*20, nrows*16))
    
    for n, plate in enumerate(batch):
        ax = axes.flatten()[n]
        
        wells = df[df["Metadata_Barcode"] == plate]
        
        usedwells = wells['Metadata_Well'].tolist()
        allwells = createwells(1)
        fillup = set(allwells) - set(usedwells)
        
        fillplate = pd.DataFrame(fillup, columns=['Metadata_Well'])
        fillplate['Metadata_Barcode'] = plate
        fillplate[str(featurename)] = np.nan
        fillplate = fillplate[["Metadata_Barcode","Metadata_Well",str(featurename)]]
        dffull = pd.concat([fillplate, wells], axis=0)
        
        dffull['col'] = dffull.Metadata_Well.astype(str).str[1:3]
        dffull['row'] = dffull.Metadata_Well.astype(str).str[0]
        
        wells_pivot = dffull.pivot(columns="col", index="row", values=str(featurename))
        
        cmap = "OrRd"
        mask = wells_pivot.isnull()
        
        sns.heatmap(wells_pivot, 
                    ax=ax,
                    vmin=0, vmax=vmax,
                    square=True,
                    cmap=cmap,  
                    linewidths=.8, linecolor='darkgray',
                    annot_kws={'fontsize':8}, 
                    cbar_kws={'label': featurename, 'orientation': 'vertical', "shrink": .5},
                    mask=mask,
                    annot=True
                    )
        
        ax.set_ylabel('x', fontsize = 8)
        ax.set_xlabel('y', fontsize = 8)
        ax.set_title(plate)
        
    plt.subplots_adjust(wspace=0.1, hspace=0.1)
    plt.suptitle(str(featurename), fontsize=50)
    plt.savefig("platemaps/level5_feature_{}.png".format(featurename))
    plt.show()
    plt.close()