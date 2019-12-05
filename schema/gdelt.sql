CREATE TABLE IF NOT EXISTS data_management_fields
(
    ID        SERIAL PRIMARY KEY,
    SOURCEURL VARCHAR(999) UNIQUE
);

CREATE TABLE IF NOT EXISTS event_geo_actor1
(
    Actor1Geo_ADM1Code    VARCHAR(999) PRIMARY KEY,
    Actor1Geo_FeatureID   INTEGER,
    Actor1Geo_CountryCode VARCHAR(999),
    Actor1_Geo_Lat        FLOAT,
    Actor1_Geo_Long       FLOAT,
    Actor1_Geo_Type       INTEGER,
    Actor1Geo_FullName    VARCHAR(999)
);

CREATE TABLE IF NOT EXISTS actor1
(
    Actor1Code           VARCHAR(999) PRIMARY KEY,
    Actor1Geo_ADM1Code   VARCHAR(999),
    Actor1Name           VARCHAR(999),
    Actor1KnownGroupCode VARCHAR(999),
    Actor1Religion1Code  VARCHAR(999),
    Actor1Religion2Code  VARCHAR(999),
    Actor1CountryCode    VARCHAR(999),
    Actor1Type1Code      VARCHAR(999),
    Actor1Type2Code      VARCHAR(999),
    Actor1Type3Code      VARCHAR(999),
    Actor1EthnicCode     VARCHAR(999),

    FOREIGN KEY (Actor1Geo_ADM1Code) REFERENCES event_geo_actor1 (Actor1Geo_ADM1Code)
);

CREATE TABLE IF NOT EXISTS event_geo_actor2
(
    Actor2Geo_ADM1Code    VARCHAR(999) PRIMARY KEY,
    Actor2Geo_FeatureID   INTEGER,
    Actor2Geo_CountryCode VARCHAR(999),
    Actor2_Geo_Lat        FLOAT,
    Actor2_Geo_Long       FLOAT,
    Actor2_Geo_Type       INTEGER,
    Actor2Geo_FullName    VARCHAR(999)
);

CREATE TABLE IF NOT EXISTS actor2
(
    Actor2Code           VARCHAR(999) PRIMARY KEY,
    Actor2Geo_ADM1Code   VARCHAR(999),
    Actor2Name           VARCHAR(999),
    Actor2KnownGroupCode VARCHAR(999),
    Actor2Religion1Code  VARCHAR(999),
    Actor2Religion2Code  VARCHAR(999),
    Actor2CountryCode    VARCHAR(999),
    Actor2Type1Code      VARCHAR(999),
    Actor2Type2Code      VARCHAR(999),
    Actor2Type3Code      VARCHAR(999),
    Actor2EthnicCode     VARCHAR(999),

    FOREIGN KEY (Actor2Geo_ADM1Code) REFERENCES event_geo_actor2 (Actor2Geo_ADM1Code)
);

CREATE TABLE IF NOT EXISTS event_geo_action
(
    ActionGeo_ADM1Code    VARCHAR(999) PRIMARY KEY,
    ActionGeo_FeatureID   INTEGER,
    ActionGeo_CountryCode VARCHAR(999),
    Action_Geo_Lat        FLOAT,
    Action_Geo_Long       FLOAT,
    Action_Geo_Type       INTEGER,
    ActionGeo_FullName    VARCHAR(999)
);

CREATE TABLE IF NOT EXISTS country
(
    Geo VARCHAR(99) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS income
(
    IID      SERIAL PRIMARY KEY,
    Geo      VARCHAR(99) NOT NULL,
    Value    INTEGER,
    Unit     VARCHAR(99),
    AgeGroup VARCHAR(999),
    Type     VARCHAR(999),
    Year     INTEGER,
    Sex      VARCHAR(999),

    FOREIGN KEY (Geo) REFERENCES country (Geo)
);

CREATE TABLE IF NOT EXISTS tourist
(
    TID          SERIAL PRIMARY KEY,
    Geo          VARCHAR(99) NOT NULL,
    Value        INTEGER,
    Time         INTEGER,
    Accomodation VARCHAR(999),
    Unit         VARCHAR(999),

    FOREIGN KEY (Geo) REFERENCES country (Geo)
);

CREATE TABLE IF NOT EXISTS influence
(
    TID SERIAL,
    IID SERIAL,

    FOREIGN KEY (TID) REFERENCES tourist (TID),
    FOREIGN KEY (IID) REFERENCES income (IID)
);

CREATE TABLE IF NOT EXISTS event_action
(
    EventCode          VARCHAR(999) PRIMARY KEY,
    Actor1Code         VARCHAR(999),
    Actor2Code         VARCHAR(999),
    ActionGeo_ADM1Code VARCHAR(999),
    TID                INTEGER,
    EventBaseCode      VARCHAR(999),
    EventRootCode      VARCHAR(999),
    IsRootEvent        BOOLEAN,
    GoldsteinScale     FLOAT,
    QuadClass          INTEGER,
    AvgTone            FLOAT,
    NumMentions        INTEGER,
    NumSources         INTEGER,
    NumArticles        INTEGER,

    FOREIGN KEY (Actor1Code) REFERENCES actor1 (Actor1Code),
    FOREIGN KEY (Actor2Code) REFERENCES actor2 (Actor2Code),
    FOREIGN KEY (ActionGeo_ADM1Code) REFERENCES event_geo_action (ActionGeo_ADM1Code),
    FOREIGN KEY (TID) REFERENCES tourist (TID)
);

CREATE TABLE IF NOT EXISTS eventid_and_date
(
    GlobalEventID INTEGER PRIMARY KEY,
    SOURCEURL     VARCHAR(999),
    EventCode     VARCHAR(999),
    FractionDate  FLOAT,
    Day           INTEGER,
    MonthYear     INTEGER,
    Year          INTEGER,

    FOREIGN KEY (SOURCEURL) REFERENCES data_management_fields (SOURCEURL),
    FOREIGN KEY (EventCode) REFERENCES event_action (EventCode)
);

