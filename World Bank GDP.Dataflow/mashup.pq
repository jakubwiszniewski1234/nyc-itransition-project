[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query1_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "indicator_id", DestinationColumnName = "indicator_id"], [SourceColumnName = "indicator_name", DestinationColumnName = "indicator_name"], [SourceColumnName = "country_iso2", DestinationColumnName = "country_iso2"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "country_iso3", DestinationColumnName = "country_iso3"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "gdp_usd", DestinationColumnName = "gdp_usd"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared FetchWorldBankGDP = let
    CurrentYear = Text.From(Date.Year(DateTimeZone.UtcNow())),
    Source = Json.Document(
        Web.Contents(
            "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
            [
                Query = [
                    #"format" = "json",
                    #"date" = "2008:" & CurrentYear,
                    #"per_page" = "5000"
                ]
            ]
        )
    ),
    DataList = Source{1},
    ConvertedToTable = Table.FromList(DataList, Splitter.SplitByNothing(), {"Record"}, null, ExtraValues.Error),
    ExpandedFields = Table.ExpandRecordColumn(
        ConvertedToTable,
        "Record",
        {"indicator", "country", "countryiso3code", "date", "value"},
        {"indicator", "country", "country_iso3", "year", "gdp_usd"}
    ),
    ExpandedIndicator = Table.ExpandRecordColumn(
        ExpandedFields,
        "indicator",
        {"id", "value"},
        {"indicator_id", "indicator_name"}
    ),
    ExpandedCountry = Table.ExpandRecordColumn(
        ExpandedIndicator,
        "country",
        {"id", "value"},
        {"country_iso2", "country_name"}
    ),
    TypedTable = Table.TransformColumnTypes(
        ExpandedCountry,
        {
            {"indicator_id", type text},
            {"indicator_name", type text},
            {"country_iso2", type text},
            {"country_name", type text},
            {"country_iso3", type text},
            {"year", Int64.Type},
            {"gdp_usd", type number}
        },
        "en-US"
    ),
    FilteredValidRows = Table.SelectRows(
        TypedTable,
        each [country_iso3] <> null and [country_iso3] <> "" and [gdp_usd] <> null
    )
in
    FilteredValidRows;
shared Query1_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "959bf169-7bd5-4462-8070-0dcab9eb4d7e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ff9da31-76e3-417f-9b47-2616293fec0c"]}[Data],
  Navigation_3 = Navigation_2{[Id = "bronze", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "WorldBankGDP", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FetchFXECBRates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FetchFXECBRates = let
    RawCsv = Web.Contents(
        "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
        [
            Query = [
                #"format" = "csvdata"
            ]
        ]
    ),
    ParsedCsv = Csv.Document(RawCsv, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ParsedCsv, [PromoteAllScalars = true]),
    SelectedColumns = Table.SelectColumns(PromotedHeaders, {"TIME_PERIOD", "OBS_VALUE", "CURRENCY", "CURRENCY_DENOM"}),
    FilteredNonEmpty = Table.SelectRows(SelectedColumns, each [OBS_VALUE] <> null and [OBS_VALUE] <> ""),
    RenamedColumns = Table.RenameColumns(FilteredNonEmpty, {
        {"TIME_PERIOD", "fx_date"},
        {"OBS_VALUE", "usd_per_eur"}
    }),

    TypedRates = Table.TransformColumnTypes(
        RenamedColumns,
        {
            {"fx_date", type date},
            {"usd_per_eur", type number}
        },
        "en-US"
    ),
    AddedPairAndFlag = Table.AddColumn(TypedRates, "is_ecb_trading_day", each true, type logical),

    MinDate = List.Min(AddedPairAndFlag[fx_date]),
    MaxDate = List.Max(AddedPairAndFlag[fx_date]),
    DayCount = Duration.Days(MaxDate - MinDate) + 1,
    CalendarTable = Table.FromList(
        List.Dates(MinDate, DayCount, #duration(1, 0, 0, 0)),
        Splitter.SplitByNothing(),
        {"fx_date"},
        null,
        ExtraValues.Error
    ),
    TypedCalendar = Table.TransformColumnTypes(CalendarTable, {{"fx_date", type date}}),
    JoinedCalendar = Table.NestedJoin(TypedCalendar, {"fx_date"}, AddedPairAndFlag, {"fx_date"}, "Fx", JoinKind.LeftOuter),
    ExpandedFx = Table.ExpandTableColumn(JoinedCalendar, "Fx", {"usd_per_eur", "CURRENCY", "CURRENCY_DENOM", "is_ecb_trading_day"}),
    FilledDown = Table.FillDown(ExpandedFx, {"usd_per_eur", "CURRENCY", "CURRENCY_DENOM"}),
    ReplacedNullFlag = Table.ReplaceValue(FilledDown, null, false, Replacer.ReplaceValue, {"is_ecb_trading_day"}),
    AddedInverseRate = Table.AddColumn(ReplacedNullFlag, "eur_per_usd", each 1 / [usd_per_eur], type number)
in
    AddedInverseRate;
shared FetchFXECBRates_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "959bf169-7bd5-4462-8070-0dcab9eb4d7e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ff9da31-76e3-417f-9b47-2616293fec0c"]}[Data],
  Navigation_3 = Navigation_2{[Id = "bronze", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "USD_EUR_Daily_Rates", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
