CREATE TABLE [gold].[DimFX] (
    [fx_key]          INT             NOT NULL,
    [usd_to_eur_rate] DECIMAL (10, 6) NOT NULL,
    [eur_to_usd_rate] DECIMAL (10, 6) NOT NULL,
    [is_trading_day]  BIT             NOT NULL,
    [date_key]        INT             NULL
);


GO

ALTER TABLE [gold].[DimFX]
    ADD CONSTRAINT [FK_DimFX_DimDate] FOREIGN KEY ([date_key]) REFERENCES [gold].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [gold].[DimFX]
    ADD CONSTRAINT [PK_DimFX] PRIMARY KEY NONCLUSTERED ([fx_key] ASC) NOT ENFORCED;


GO