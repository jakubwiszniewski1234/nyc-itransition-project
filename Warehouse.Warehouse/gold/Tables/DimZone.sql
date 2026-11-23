CREATE TABLE [gold].[DimZone] (
    [zone_key]     INT           NOT NULL,
    [borough]      VARCHAR (50)  NOT NULL,
    [zone_name]    VARCHAR (100) NOT NULL,
    [service_zone] VARCHAR (50)  NOT NULL,
    [latitude]     FLOAT (53)    NULL,
    [longitude]    FLOAT (53)    NULL
);


GO

ALTER TABLE [gold].[DimZone]
    ADD CONSTRAINT [PK_DimZone] PRIMARY KEY NONCLUSTERED ([zone_key] ASC) NOT ENFORCED;


GO