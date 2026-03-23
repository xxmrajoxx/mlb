from pybaseball import statcast_pitcher

df = statcast_pitcher(
    start_dt="2025-03-01",
    end_dt="2025-03-07",
    player_id=592789   
)


print(df.head())
print(df.shape)
print(df.columns.tolist())


# https://github.com/jldbc/pybaseball/blob/master/docs/statcast_pitcher.md