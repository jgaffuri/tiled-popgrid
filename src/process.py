from pygridmap import gridtiler
from datetime import datetime
import os


inpath = "/home/juju/geodata/gisco/grids/"
transform = True
tiling = False


#transform
if transform:
    def tr(c):
        p2006 = float(c["TOT_P_2006"])
        p2011 = float(c["TOT_P_2011"])
        p2018 = float(c["TOT_P_2018"])
        p2021 = float(c["TOT_P_2021"])
        if p2006==0 and p2011==0 and p2018==0 and p2021==0: return False
        x = c["X_LLC"]
        y = c["Y_LLC"]
        c.clear()
        c["x"]=x
        c["y"]=y
        c["p2006"]=p2006
        c["p2011"]=p2011
        c["p2018"]=p2018
        c["p2021"]=p2021
        #print(c)

    for resolution in [ 100, 50, 20, 10, 5, 2, 1 ]:
        print("Transform", resolution)
        gridtiler.grid_transformation(inpath+"grid_"+str(resolution)+"km.csv", tr, "./tmp/"+str(resolution*1000)+".csv")


#tiling
if tiling:
    for resolution in [1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        print("tiling for resolution", resolution)

        #create output folder
        out_folder = 'pub/v2/parquet_total/' + str(resolution)
        if not os.path.exists(out_folder): os.makedirs(out_folder)

        gridtiler.grid_tiling(
            aggregated_folder+str(resolution)+".csv",
            out_folder,
            resolution,
            tile_size_cell = 512,
            x_origin = 0,
            y_origin = 0,
            format = "parquet"
        )


