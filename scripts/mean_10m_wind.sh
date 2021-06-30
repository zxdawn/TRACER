# example for calculate mean 10 m wind of era5_Land data
#!/bin/bash
cdo -fldmean -sellonlatbox,-89,-92,26,28 era5_201912.nc era5_201912_gulf_of_mexico.nc
cdo -fldmean -sellonlatbox,-118,-125,27,32 era5_201912.nc era5_201912_san_diego.nc
cdo -fldmean -sellonlatbox,-123,-129,31,38 era5_201912.nc era5_201912_los_angeles.nc

cdo -fldmean -sellonlatbox,-95.1,-95.5,29.6,29.9 era5_201912.nc era5_201912_huston.nc
cdo -fldmean -sellonlatbox,-118.1,-118.4,33.95,34.15 era5_201912.nc era5_201912_los_angeles_city.nc
cdo -fldmean -sellonlatbox,-123.3,-123.7,48.3,48.6 era5_201912.nc era5_201912_victoria.nc

# ................
cdo -b 32 mergetime *gulf_of_mexico.nc era5_gulf_of_mexico_201912-202006.nc
cdo -b 32 mergetime *san_diego.nc era5_san_diego_201912-202006.nc
cdo -b 32 mergetime *los_angeles.nc era5_los_angeleso_201912-202006.nc
