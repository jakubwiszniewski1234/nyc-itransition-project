CREATE TABLE [gold].[FactAirQualityDaily] (
    [date_key]            INT        NOT NULL,
    [zone_key]            INT        NOT NULL,
    [avg_pm25]            FLOAT (53) NULL,
    [min_pm25]            FLOAT (53) NULL,
    [max_pm25]            FLOAT (53) NULL,
    [avg_no2]             FLOAT (53) NULL,
    [min_no2]             FLOAT (53) NULL,
    [max_no2]             FLOAT (53) NULL,
    [active_sensor_count] INT        NOT NULL,
    [is_pollution_spike]  BIT        NOT NULL
);


GO

ALTER TABLE [gold].[FactAirQualityDaily]
    ADD CONSTRAINT [FK_FactAirQualityDaily_DimDate] FOREIGN KEY ([date_key]) REFERENCES [gold].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactAirQualityDaily]
    ADD CONSTRAINT [FK_FactAirQualityDaily_DimZone] FOREIGN KEY ([zone_key]) REFERENCES [gold].[DimZone] ([zone_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactAirQualityDaily]
    ADD CONSTRAINT [PK_FactAirQualityDaily] PRIMARY KEY NONCLUSTERED ([date_key] ASC, [zone_key] ASC) NOT ENFORCED;


GO