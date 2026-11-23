CREATE TABLE [gold].[FactTaxiDaily] (
    [pickup_date_key]              INT             NOT NULL,
    [pickup_zone_key]              INT             NOT NULL,
    [dropoff_zone_key]             INT             NOT NULL,
    [fx_key]                       INT             NOT NULL,
    [gdp_key]                      INT             NOT NULL,
    [taxi_type]                    VARCHAR (10)    NOT NULL,
    [is_shared_ride]               BIT             NOT NULL,
    [trip_count]                   BIGINT          NOT NULL,
    [clean_fare_trip_count]        BIGINT          NOT NULL,
    [measured_distance_trip_count] BIGINT          NOT NULL,
    [total_revenue_usd]            DECIMAL (14, 2) NOT NULL,
    [total_distance_miles]         DECIMAL (14, 2) NOT NULL,
    [total_duration_minutes]       DECIMAL (14, 2) NOT NULL
);


GO

ALTER TABLE [gold].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_DimDate] FOREIGN KEY ([pickup_date_key]) REFERENCES [gold].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_DimFX] FOREIGN KEY ([fx_key]) REFERENCES [gold].[DimFX] ([fx_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_DimGDP] FOREIGN KEY ([gdp_key]) REFERENCES [gold].[DimGDP] ([gdp_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_DropoffZone] FOREIGN KEY ([dropoff_zone_key]) REFERENCES [gold].[DimZone] ([zone_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_PickupZone] FOREIGN KEY ([pickup_zone_key]) REFERENCES [gold].[DimZone] ([zone_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[FactTaxiDaily]
    ADD CONSTRAINT [PK_FactTaxiDaily] PRIMARY KEY NONCLUSTERED ([pickup_date_key] ASC, [pickup_zone_key] ASC, [dropoff_zone_key] ASC, [taxi_type] ASC, [is_shared_ride] ASC) NOT ENFORCED;


GO