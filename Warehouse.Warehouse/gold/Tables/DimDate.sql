CREATE TABLE [gold].[DimDate] (
    [date_key]    INT          NOT NULL,
    [date]        DATE         NOT NULL,
    [year]        INT          NOT NULL,
    [month]       INT          NOT NULL,
    [month_name]  VARCHAR (15) NOT NULL,
    [day]         INT          NOT NULL,
    [day_of_week] INT          NOT NULL,
    [is_weekend]  BIT          NOT NULL,
    [day_name]    VARCHAR (15) NULL
);


GO

ALTER TABLE [gold].[DimDate]
    ADD CONSTRAINT [PK_DimDate] PRIMARY KEY NONCLUSTERED ([date_key] ASC) NOT ENFORCED;


GO