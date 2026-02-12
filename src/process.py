from pygridmap import gridtiler
from datetime import datetime
import os


inpath = "/home/juju/geodata/gisco/grids/"
transform = True
tiling = True
#True False

# make tmp folder
os.makedirs("./tmp/", exist_ok=True)

# transform
if transform:

    # one per year
    def make_tr(year):
        def tr(c):
            # no UK
            if "UK" == c["CNTR_ID"]: return False

            p = float(c["TOT_P_"+str(year)])
            if p==0: return False
            x = c["X_LLC"]
            y = c["Y_LLC"]
            cnt = c["CNTR_ID"]
            c.clear()
            c["x"]=x
            c["y"]=y
            c["T"]=p
            c["CNTR_ID"] = cnt
        return tr

    for resolution in [ 100, 50, 20, 10, 5, 2, 1 ]:
        for year in [ 2021, 2018, 2011, 2006 ]:
            print("Transform", year, resolution)
            gridtiler.grid_transformation(inpath+"grid_"+str(resolution)+"km.csv", make_tr(year), "./tmp/"+str(year)+"_"+str(resolution*1000)+".csv")

    # one with all years, to map change
    def tr(c):
        cntid = c["CNTR_ID"]
        #cnts = cntid.split("-")

        # no UK
        if "UK" == cntid: return False

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
        c["T2006"]=p2006
        c["T2011"]=p2011
        c["T2018"]=p2018
        c["T2021"]=p2021

        # remove pop for 2011 and 2021, to make it impossible to compute change for those countries
        for cs in ["IS","BA","RS","AL","MK","ME", "BA-RS", "BA-ME-RS", "BA-ME", "ME-RS", "MK-RS", "AL-MK","AL-RS","AL-MK-RS", "AL-ME-RS", "AD", "MC", "SM"]:
            if cs == cntid:
                c["T2011"]=None
                c["T2021"]=None



    for resolution in [ 100, 50, 20, 10, 5, 2, 1 ]:
        print("Transform", resolution)
        gridtiler.grid_transformation(inpath+"grid_"+str(resolution)+"km.csv", tr, "./tmp/"+str(resolution*1000)+".csv")


# tiling
if tiling:

    resolutions = [ 100000, 50000, 20000, 10000, 5000, 2000, 1000 ]
    years = [ 2021, 2018, 2011, 2006 ]

    for resolution in resolutions:
        for year in years:
            print("tiling", year, resolution)

            #create output folder
            out_folder = 'pub/v1/'+str(year)+'/' + str(resolution)
            if not os.path.exists(out_folder): os.makedirs(out_folder)

            gridtiler.grid_tiling(
                "./tmp/"+str(year)+"_"+str(resolution)+".csv",
                out_folder,
                resolution,
                tile_size_cell = 512,
                x_origin = 0,
                y_origin = 0,
                format = "parquet"
            )


    for resolution in resolutions:
        print("tiling", year, resolution)

        #create output folder
        out_folder = 'pub/v1/change/' + str(resolution)
        if not os.path.exists(out_folder): os.makedirs(out_folder)

        gridtiler.grid_tiling(
            "./tmp/"+str(resolution)+".csv",
            out_folder,
            resolution,
            tile_size_cell = 256,
            x_origin = 0,
            y_origin = 0,
            format = "parquet"
        )
