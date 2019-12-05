CREATE TABLE IF NOT EXISTS data_management_fields
(
    DATEADDED SERIAL PRIMARY KEY,
    SOURCEURL VARCHAR(999) UNIQUE
);

CREATE TABLE IF NOT EXISTS event_geo
(
    ADM1Code    VARCHAR(999) PRIMARY KEY,
    FeatureID   INTEGER,
    CountryCode VARCHAR(999),
    Lat         FLOAT,
    Long        FLOAT,
    Type        INTEGER,
    FullName    VARCHAR(999)
);

CREATE TABLE IF NOT EXISTS actor
(
    Code           VARCHAR(999) PRIMARY KEY,
    Name           VARCHAR(999),
    KnownGroupCode VARCHAR(999),
    Religion1Code  VARCHAR(999),
    Religion2Code  VARCHAR(999),
    CountryCode    VARCHAR(999),
    Type1Code      VARCHAR(999),
    Type2Code      VARCHAR(999),
    Type3Code      VARCHAR(999),
    EthnicCode     VARCHAR(999)
);

CREATE TABLE IF NOT EXISTS actor1
(
    ADM1Code VARCHAR(999),
    FOREIGN KEY (ADM1Code) REFERENCES event_geo (ADM1Code)
) INHERITS (actor);



CREATE TABLE IF NOT EXISTS actor2
(
    ADM1Code VARCHAR(999),
    FOREIGN KEY (ADM1Code) REFERENCES event_geo (ADM1Code)
) INHERITS (actor);

CREATE TABLE IF NOT EXISTS country
(
    CID SERIAL PRIMARY KEY,
    Geo VARCHAR(99) NOT NULL
);

CREATE TABLE IF NOT EXISTS income
(
    IID      SERIAL PRIMARY KEY,
    CID      SERIAL NOT NULL,
    Value    INTEGER,
    Unit     VARCHAR(99),
    AgeGroup VARCHAR(999),
    Type     VARCHAR(999),
    Year     INTEGER,
    Sex      VARCHAR(999),

    FOREIGN KEY (CID) REFERENCES country (CID)
);

CREATE TABLE IF NOT EXISTS tourist
(
    TID           SERIAL PRIMARY KEY,
    CID           SERIAL NOT NULL,
    Value         INTEGER,
    Time          INTEGER,
    Accommodation VARCHAR(999),
    Unit          VARCHAR(999),

    FOREIGN KEY (CID) REFERENCES country (CID)
);

CREATE TABLE IF NOT EXISTS influence_income
(
    TID SERIAL,
    IID SERIAL,

    FOREIGN KEY (TID) REFERENCES tourist (TID),
    FOREIGN KEY (IID) REFERENCES income (IID)
);

CREATE TABLE IF NOT EXISTS event_action
(
    EventCode      VARCHAR(999) PRIMARY KEY,
    Actor1Code     VARCHAR(999),
    Actor2Code     VARCHAR(999),
    ADM1Code       VARCHAR(999),
    TID            INTEGER,
    EventBaseCode  VARCHAR(999),
    EventRootCode  VARCHAR(999),
    IsRootEvent    BOOLEAN,
    GoldsteinScale FLOAT,
    QuadClass      INTEGER,
    AvgTone        FLOAT,
    NumMentions    INTEGER,
    NumSources     INTEGER,
    NumArticles    INTEGER,

    FOREIGN KEY (Actor1Code) REFERENCES actor1 (Code),
    FOREIGN KEY (Actor2Code) REFERENCES actor2 (Code),
    FOREIGN KEY (ADM1Code) REFERENCES event_geo (ADM1Code),
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

