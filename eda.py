# coding: utf-8
import polars as pl
try:
    rides = pl.read_csv("data/20*.csv")
except:
    pass
rides = pl.read_csv("data/20*.csv", columns=["starttime", "start station id", "end station id", "start station name", "end station name"])
try:
    ride_stations = set(rides.select([pl.col("start station name")])) | set(rides.select([pl.col("start station name")]))
except:
    pass
ride_stations = set()
try:
    for column in ["start station name", "end station name"]:
        ride_stations += set(rides.select(pl.col(column).tolist()))
except:
    pass


try:
    for column in ["start station name", "end station name"]:
        ride_stations += set(rides.select(pl.col(column).to_list()))
except:
    pass

try:
    for column in ["start station name", "end station name"]:
        ride_stations += set(rides.select(pl.col(column)).to_list())
except:
    pass

try:
    for column in ["start station name", "end station name"]:
        ride_stations += set(rides.get_column(column).to_list())
except:
    pass

for column in ["start station name", "end station name"]:
    ride_stations |= set(rides.get_column(column).to_list())
len(ride_stations)
ride_stations
stations = pl.read_csv("data/current_bluebikes_stations.csv", skip_rows=1, columns=["Number", "Name", "District", "Public"])
cambridge_stations = stations.filter(pl.col("District") == "Cambridge")
try:
    cambridge_rides = rides.filter(pl.col("start station name").is_in(cambridge_stations) | pl.col("end station name").is_in(cambridge_stations))
except:
    pass
cambridge_stations = cambridge_stations.get_column("Name")
try:
    cambridge_rides = rides.filter(pl.col("start station name").is_in(cambridge_stations) | pl.col("end station name").is_in(cambridge_stations))
except:
    pass
cambridge_rides
pl.Config.set_tbl_width_chars(60)
cambridge_rides
def by_year(df):
    return df.groupby_dynamic("starttime", every="1y").agg(pl.count())
try:
    by_year(cambridge_rides)
except:
    pass
cambridge_rides = cambridge_rides.with_columns(pl.col("starttime").str.slice(0, 10).str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False)).sort("starttime")
cambridge_rides
by_year(cambridge_rides)
by_year(cambridge_rides.filter(pl.col("starttime").dt.year() < 2023))
