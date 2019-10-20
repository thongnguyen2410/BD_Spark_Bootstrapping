CREATE EXTERNAL TABLE mlb_heights (
    Percentage INT,
    Catcher_mErr DOUBLE,
    Catcher_vErr DOUBLE,
    Designated_Hitter_mErr DOUBLE,
    Designated_Hitter_vErr DOUBLE,
    First_Baseman_mErr DOUBLE,
    First_Baseman_vErr DOUBLE,
    Outfielder_mErr DOUBLE,
    Outfielder_vErr DOUBLE,
    Relief_Pitcher_mErr DOUBLE,
    Relief_Pitcher_vErr DOUBLE,
    Second_Baseman_mErr DOUBLE,
    Second_Baseman_vErr DOUBLE,
    Shortstop_mErr DOUBLE,
    Shortstop_vErr DOUBLE,
    Starting_Pitcher_mErr DOUBLE,
    Starting_Pitcher_vErr DOUBLE,
    Third_Baseman_mErr DOUBLE,
    Third_Baseman_vErr DOUBLE
)
ROW FORMAT
    DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
LOCATION '/user/cloudera/csv';