import polars as pl

stations = pl.read_csv(
    "data/current_bluebikes_stations.csv",
    skip_rows=1,
    columns=["Number", "Name", "District", "Public"],
)
cambridge_stations = stations.filter(
    pl.col("District") == "Cambridge"
).get_column("Name")

# Switched to lazy API:
rides = (
    pl.scan_csv("data/20*.csv")
    .with_columns(
        pl.col("starttime")
        .str.slice(0, 10)
        .str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False)
    )
)
cambridge_rides = rides.filter(
    pl.col("start station name").is_in(cambridge_stations)
    | pl.col("end station name").is_in(cambridge_stations)
)


def by_year(df):
    return df.groupby_dynamic("starttime", every="1y").agg(
        pl.count()
    )


print(
    str(
        by_year(cambridge_rides)
        .filter(pl.col("starttime").dt.year() < 2023)
        .collect()
    )
)
