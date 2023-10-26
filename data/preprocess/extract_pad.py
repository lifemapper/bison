import glob
import os
import zipfile

pth = "/mnt/sata8/bison/2023/big_data/geodata/pad_st"
zip_pattern = os.path.join(pth, "PADUS3_0_State_*_SHP.zip")
designation_pattern = "PADUS3_0Designation_State"
desig_prj_pattern = os.path.join(pth, f"{designation_pattern}??_4326.shp")


zfilenames = glob.glob(zip_pattern)

# Extract only the Designation files for each
for zfname in zfilenames:
    zipbasename, _ = os.path.splitext(os.path.basename(zfname))
    with zipfile.ZipFile(zfname) as zf:
        for info in zf.infolist():
            basename = os.path.basename(info.filename)
            if basename.startswith(designation_pattern):
                zf.extract(info, pth)

# TODO: Script project to 4326

# Zip up state designation shapefiles
state_desig_shpfilenames = glob.glob(desig_prj_pattern)
for shpfname in state_desig_shpfilenames:
    basename, _ = os.path.splitext(os.path.basename(shpfname))
    curr_fnames = glob.glob(os.path.join(pth, f"{basename}.???"))
    with zipfile.ZipFile(f"{basename}.zip", mode="w") as archive:
        for fname in curr_fnames:
            archive.write(fname, arcname=fname.name)