CREATE TABLE [gold].[DimGDP] (
    [gdp_key]      INT             NOT NULL,
    [country_iso3] VARCHAR (3)     NOT NULL,
    [country_name] VARCHAR (50)    NOT NULL,
    [year]         INT             NOT NULL,
    [gdp_usd]      DECIMAL (10, 2) NOT NULL
);


GO