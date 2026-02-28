"""Merge HVFHV route data with NYC crime data and assign crime deciles.

Steps:
  1. Spatially join crime points to taxi zone polygons.
  2. Compute crime rate (complaints per sq km) per zone.
  3. Merge crime rates onto trips by both pickup and dropoff zone.
  4. Compute pct_time_increase = (actual - estimated) / estimated * 100.
  5. Drop outliers beyond ±3 SD, assign pu and do crime deciles.
"""

import os

import geopandas as gpd
import pandas as pd

INPUT_DIR = "../input"
OUTPUT_DIR = "../output"


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    trips = pd.read_parquet(f"{INPUT_DIR}/trips_with_routes.parquet")

    crime = pd.read_csv(f"{INPUT_DIR}/nyc_crime.csv")
    crime["latitude"] = pd.to_numeric(crime["latitude"], errors="coerce")
    crime["longitude"] = pd.to_numeric(crime["longitude"], errors="coerce")
    crime = crime.dropna(subset=["latitude", "longitude"])

    zones_gdf = gpd.read_file(f"{INPUT_DIR}/taxi_zones.geojson").to_crs("EPSG:4326")
    id_col = next(
        c for c in zones_gdf.columns
        if c.lower().replace("_", "") in ("locationid", "locationi")
    )
    zones_gdf = zones_gdf.rename(columns={id_col: "LocationID"})
    zones_gdf["LocationID"] = pd.to_numeric(zones_gdf["LocationID"], errors="coerce").astype("Int64")

    # Compute zone area in sq km using a projected CRS (EPSG:32618 = UTM zone 18N, covers NYC)
    zones_area = zones_gdf.to_crs("EPSG:32618").copy()
    zones_area["area_km2"] = zones_area.geometry.area / 1e6
    zones_gdf = zones_gdf.merge(
        zones_area[["LocationID", "area_km2"]], on="LocationID", how="left"
    )

    # Spatial join: assign each crime record to a taxi zone
    crime_gdf = gpd.GeoDataFrame(
        crime,
        geometry=gpd.points_from_xy(crime["longitude"], crime["latitude"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(
        crime_gdf,
        zones_gdf[["LocationID", "geometry"]],
        how="left",
        predicate="within",
    )

    crime_by_zone = (
        joined.groupby("LocationID")
        .size()
        .reset_index(name="crime_count")
    )
    crime_by_zone["LocationID"] = crime_by_zone["LocationID"].astype(int)

    # Merge area and compute crimes per sq km
    zone_meta = zones_gdf[["LocationID", "area_km2"]].copy()
    zone_meta["LocationID"] = zone_meta["LocationID"].astype(int)
    crime_by_zone = crime_by_zone.merge(zone_meta, on="LocationID", how="left")
    crime_by_zone["crime_rate_km2"] = crime_by_zone["crime_count"] / crime_by_zone["area_km2"]
    # Merge by pickup zone
    crime_pu = crime_by_zone.rename(columns={"LocationID": "PULocationID"})
    trips["PULocationID"] = trips["PULocationID"].astype(int)
    trips = trips.merge(crime_pu[["PULocationID", "crime_rate_km2"]], on="PULocationID", how="left")
    trips["crime_rate_km2"] = trips["crime_rate_km2"].fillna(0)

    # Merge by dropoff zone
    crime_do = crime_by_zone.rename(columns={"LocationID": "DOLocationID",
                                             "crime_rate_km2": "do_crime_rate_km2"})
    trips["DOLocationID"] = trips["DOLocationID"].astype(int)
    trips = trips.merge(crime_do[["DOLocationID", "do_crime_rate_km2"]], on="DOLocationID", how="left")
    trips["do_crime_rate_km2"] = trips["do_crime_rate_km2"].fillna(0)

    # Percentage trip time increase: (actual - estimated) / estimated * 100
    trips["pct_time_increase"] = (trips["time_diff"] / trips["estimated_duration"]) * 100

    # Drop outliers beyond ±3 SD
    mean, std = trips["pct_time_increase"].mean(), trips["pct_time_increase"].std()
    trips = trips[trips["pct_time_increase"].between(mean - 3 * std, mean + 3 * std)].copy()

    # Crime deciles: 1 (lowest rate) to 10 (highest rate)
    trips["crime_decile"] = (
        pd.qcut(trips["crime_rate_km2"], q=10, labels=False, duplicates="drop") + 1
    )
    trips["do_crime_decile"] = (
        pd.qcut(trips["do_crime_rate_km2"], q=10, labels=False, duplicates="drop") + 1
    )

    out_path = f"{OUTPUT_DIR}/analysis_data.parquet"
    trips.to_parquet(out_path, index=False)
    print(f"Saved {len(trips):,} rows to {out_path}")


if __name__ == "__main__":
    main()
