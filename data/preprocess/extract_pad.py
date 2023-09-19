import glob
import os
import zipfile

pth = "/mnt/sata8/bison/2023/big_data/geodata/pad_st"
zip_pattern = os.path.join(pth, "PADUS3_0_State_*_SHP.zip")
designation_pattern = "PADUS3_0Designation_State"

zfilenames = glob.glob(zip_pattern)

for zfname in zfilenames:
    zipbasename, _ = os.path.splitext(os.path.basename(zfname))
    with zipfile.ZipFile(zfname) as zf:
        for info in zf.infolist():
            basename = os.path.basename(info.filename)
            if basename.startswith(designation_pattern):
                zf.extract(info, pth)

