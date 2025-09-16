from pygridmap import gridtiler
from datetime import datetime
import os

# TODO

"/home/juju/geodata/gisco/grids/grid_10km.csv"






aggregated_folder = "/home/juju/Bureau/aggregated/"
if not os.path.exists(aggregated_folder): os.makedirs(aggregated_folder)

transform = True
aggregate = True
tiling = True


#transform

if transform:
    def tr(c):

        # skip non populated non confidential cells
        pop = c['T']
        ci = c['T_CI']
        if pop == "0" and ci != "-9999": return False
        #if pop == "0" and ci == "-9999": print("ok!")

        gid = c['GRD_ID'].replace("CRS3035RES1000mN", "").split('E')

        c.clear()
        c['T'] = pop
        c['x'] = gid[1]
        c['y'] = gid[0]

        #set confidentiality to 0 or 1
        if ci == "": c['T_CI'] = 0
        elif ci == "-9999": c['T_CI'] = 1
        else: print("Unexpected T_CI: ", ci)

        #initialise nb - to count the number of cells aggregated
        c['nb'] = 1

    gridtiler.grid_transformation("/home/juju/geodata/census/2021/ESTAT_Census_2021_V2.csv", tr, aggregated_folder+"1000.csv")


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


