"""Script to extract Designation from state PAD datasets, reproject, then zip."""

# Make sure that gdal is installed on the system and that the executable ogr2ogr
#   exists in the path
# Run this script in the input/output directory (containing the data)
import glob
import os
import subprocess
import zipfile

# pth = "/home/astewart/git/bison/data/big_data/geodata/pad_st"
epsg = 4326
zip_pattern = "PADUS3_0_State_*_SHP.zip"
designation_pattern = "PADUS3_0Designation_State"
desig_shp_pattern = f"{designation_pattern}??.shp"
prj_postfix = f"_{epsg}.shp"
prj_desig_shp_pattern = f"{designation_pattern}??{prj_postfix}"

# Extract only the Designation files for each
zfilenames = glob.glob(zip_pattern)
for zfname in zfilenames:
    zipbasename, _ = os.path.splitext(os.path.basename(zfname))
    with zipfile.ZipFile(zfname) as zf:
        for info in zf.infolist():
            basename = os.path.basename(info.filename)
            if basename.startswith(designation_pattern):
                zf.extract(info)

# TODO: test project to 4326
# ogr2ogr -t_srs EPSG:4326  new_proj_shpfile  old_shpfile
desig_shpfilenames = glob.glob(desig_shp_pattern)
for orig_shpfname in desig_shpfilenames:
    basename, ext = os.path.splitext(orig_shpfname)
    new_shpfname = f"{basename}{prj_postfix}"
    if os.path.exists(new_shpfname):
        print(f"{new_shpfname} already exists")
    else:
        cmd = f"ogr2ogr -t_srs EPSG:4326 {new_shpfname} {orig_shpfname}"
        info, err = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if err:
            print(f"ogr2ogr returned error: {err}")
        else:
            print(f"Created {new_shpfname}")

# Zip up state designation shapefiles
prj_desig_shpfilenames = glob.glob(prj_desig_shp_pattern)
for sd_shpfname in prj_desig_shpfilenames:
    basename, _ = os.path.splitext(sd_shpfname)
    if os.path.exists(f"{basename}.zip"):
        print(f"{basename}.zip already exists")
    else:
        curr_shpfnames = glob.glob(f"{basename}.???")
        with zipfile.ZipFile(f"{basename}.zip", mode="w") as zip_obj:
            for fname in curr_shpfnames:
                zip_obj.write(fname)
        print(f"Created {basename}.zip")
