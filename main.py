import geopandas as gpd

shp_path = "taxi_zones/taxi_zones.shp"
gdf = gpd.read_file(shp_path)

print("Columns:", list(gdf.columns))

# centroid в исходной проекции (EPSG:2263)
gdf["centroid"] = gdf.geometry.centroid

# переводим центроиды в EPSG:4326
centroids_4326 = gpd.GeoSeries(gdf["centroid"], crs=gdf.crs).to_crs(epsg=4326)

gdf["zone_lon"] = centroids_4326.x
gdf["zone_lat"] = centroids_4326.y

cols = ["LocationID", "borough", "zone", "zone_lat", "zone_lon"]
if "service_zone" in gdf.columns:
    cols.insert(3, "service_zone")  # вставим после zone

out = gdf[cols]

out.to_parquet("nyc_taxi_zones_4326_centroids.parquet", index=False)
print("Saved parquet rows:", len(out))
