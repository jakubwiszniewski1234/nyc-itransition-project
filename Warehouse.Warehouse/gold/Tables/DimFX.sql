CREATE TABLE [gold].[DimFX] (
    [fx_key]          INT             NOT NULL,
    [date]            DATE            NOT NULL,
    [usd_to_eur_rate] DECIMAL (10, 6) NOT NULL,
    [eur_to_usd_rate] DECIMAL (10, 6) NOT NULL,
    [is_trading_day]  BIT             NOT NULL
);


GO