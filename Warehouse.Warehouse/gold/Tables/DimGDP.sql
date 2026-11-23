CREATE TABLE [gold].[DimGDP] (
    [gdp_key]      INT             NOT NULL,
    [country_iso3] VARCHAR (3)     NOT NULL,
    [country_name] VARCHAR (50)    NOT NULL,
    [year]         INT             NOT NULL,
    [gdp_usd]      DECIMAL (18, 2) NOT NULL
);


GO

ALTER TABLE [gold].[DimGDP]
    ADD CONSTRAINT [PK_DimGDP] PRIMARY KEY NONCLUSTERED ([gdp_key] ASC) NOT ENFORCED;


GO