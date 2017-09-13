Hi Bex,

I have attached a jupyter notebook where I calculated the mass residual rainfall curves. I thought long and hard about how to go about producing the composite image and in the end I decided to do the following for dry years:

-calculate the driest year (using whatever criteria) and produce an xarray with these years for each cell
-for each year within the set, produce a netcdf file where all other years are masked out
-open each netcdf file in qgis and vectorise it and save as a shape file
-use the shapefiles to define the areas of interest in calculating the geomedian for a dry period during the year (August to November in the NW)

I don't particularly think it is any better than the way you described. I am still waiting on the job on raijin to make sure that it worked.

Cheers

Neil

