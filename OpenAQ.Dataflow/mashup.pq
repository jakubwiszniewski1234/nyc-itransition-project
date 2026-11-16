[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared ApiKey = "9bd146e01dac4af8e74cd882d276c4280fb36c614b8a174e36961ff3b5854ff3" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared NYC_Locations = let
  Source = Json.Document(Web.Contents("https://api.openaq.org/v3/locations", 
        [
            Headers = [
                #"X-API-Key" = ApiKey
            ],
            Query = [
                #"bbox" = "-74.27,40.49,-73.68,40.92",
                #"monitor" = "true",
                #"limit" = "1000"
            ]
        ])
    ),
  results = Source[results],
  #"Converted to Table" = Table.FromList(results, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"id", "name", "locality", "coordinates", "isMonitor", "provider", "datetimeFirst", "datetimeLast", "sensors"}, {"location_id", "location_name", "locality", "coordinates", "isMonitor", "provider", "datetimeFirst", "datetimeLast", "sensors"}),
  #"Expanded provider" = Table.ExpandRecordColumn(#"Expanded Column1", "provider", {"name"}, {"provider_name"}),
  #"Expanded coordinates" = Table.ExpandRecordColumn(#"Expanded provider", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Added FirstYear" = Table.AddColumn(#"Expanded coordinates", "FirstYear", each try Date.Year(DateTimeZone.From([datetimeFirst][utc])) otherwise null, Int64.Type),
  #"Added LastYear" = Table.AddColumn(#"Added FirstYear", "LastYear", each try Date.Year(DateTimeZone.From([datetimeLast][utc])) otherwise null, Int64.Type),
  Custom = #"Added LastYear"
in
  Custom;
shared GetLocationsAfterYear = (TargetYear as number) =>
let
    Source = NYC_Locations,
    SelectedYears = Table.SelectRows(Source, each [isMonitor] = true and [FirstYear] <> null and [FirstYear] <= TargetYear and [LastYear] >= TargetYear)
in
    SelectedYears;
shared ExpandSensorData = (TargetYear as number) => 
let
  #"Filtered Active Locations" = GetLocationsAfterYear(TargetYear),
  #"Expanded sensors" = Table.ExpandListColumn(#"Filtered Active Locations", "sensors"),
  #"Expanded sensors 1" = Table.ExpandRecordColumn(#"Expanded sensors", "sensors", {"id", "parameter"}, {"sensor_id", "sensor_param"}),
  #"Expanded sensor_param" = Table.ExpandRecordColumn(#"Expanded sensors 1", "sensor_param", {"name"}, {"target_param"}),
  #"Filtered Pollutants" = Table.SelectRows(#"Expanded sensor_param", each List.Contains({"pm25", "no2", "o3"}, [target_param])),
  #"Cleaned Types" = Table.TransformColumnTypes(#"Filtered Pollutants", {{"location_id", Int64.Type}, {"location_name", type text}, {"locality", type text}, {"provider_name", type text}, {"latitude", type number}, {"longitude", type number}, {"sensor_id", type text}}),
  #"Added Year" = Table.AddColumn(#"Cleaned Types", "Year", each TargetYear, Int64.Type),
  Custom = #"Added Year"
in
  Custom;
shared FetchSensorData = (sensor_id as text, DateFrom as date, DateTo as date) =>
let
    FromStr = Date.ToText(DateFrom, [Format = "yyyy-MM-dd", Culture = "en-US"]),
    ToStr = Date.ToText(DateTo, [Format = "yyyy-MM-dd", Culture = "en-US"]),

    MaxRetries = 2,
    Attempts = List.Generate(
        () => [Attempt = 0, Done = false, PrevDone = false, Status = 0, Body = null],
        each not [PrevDone] and [Attempt] <= MaxRetries,
        each
            let
                NextAttempt = [Attempt] + 1,
                DelaySeconds = if [Attempt] = 0 then 1.05 else Number.Power(2, [Attempt]) * 2,
                RawCall = Function.InvokeAfter(
                    () => try Web.Contents(
                        "https://api.openaq.org",
                        [
                            RelativePath = "v3/sensors/" & Text.From(sensor_id) & "/days",
                            Headers = [#"X-API-Key" = ApiKey],
                            Query = [
                                #"date_from" = FromStr,
                                #"date_to" = ToStr,
                                #"limit" = "1000",
                                #"page" = "1"
                            ],
                            ManualStatusHandling = {408, 429, 500, 502, 503, 504},
                            Timeout = #duration(0, 0, 0, 30),
                            IsRetry = [Attempt] > 0
                        ]
                    ),
                    #duration(0, 0, 0, DelaySeconds)
                ),
                HttpStatus =
                    if RawCall[HasError] then
                        408
                    else
                        Value.Metadata(RawCall[Value])[Response.Status],
                CheckedStatus =
                    if HttpStatus = 429 then
                        error Error.Record("RateLimitHit", "OpenAQ 429 rate limit hit.")
                    else
                        HttpStatus,
                IsTransient = List.Contains({408, 500, 502, 503, 504}, CheckedStatus),
                BufferedBody =
                    if CheckedStatus = 200 and not RawCall[HasError] then
                        Json.Document(Binary.Buffer(RawCall[Value]))
                    else
                        null
            in
                [
                    Attempt = NextAttempt,
                    Done = not IsTransient,
                    PrevDone = [Done],
                    Status = CheckedStatus,
                    Body = BufferedBody
                ],
        each [Status = [Status], Body = [Body]]
    ),

    LastResp = List.Last(Attempts),
    Results =
        if LastResp[Status] = 200 and LastResp[Body] <> null and Record.HasFields(LastResp[Body], "results") then
            LastResp[Body][results]
        else
            {},

    OutputSchema = type table [
        sensor_id = text,
        date_utc = date,
        parameter = text,
        daily_avg = number,
        daily_min = number,
        daily_max = number,
        units = text,
        percent_complete = number
    ],

    ResultTable =
        if List.IsEmpty(Results) then
            #table(OutputSchema, {})
        else
            let
                ToTable = Table.FromList(Results, Splitter.SplitByNothing(), {"Record"}, null, ExtraValues.Error),
                ParsedRows = Table.AddColumn(ToTable, "Row", each [
                    sensor_id = Text.From(sensor_id),
                    date_utc = try Date.From(DateTimeZone.From([Record][period][datetimeFrom][utc])) otherwise null,
                    parameter = try Text.From([Record][parameter][name]) otherwise null,
                    daily_avg = try Number.From([Record][value]) otherwise null,
                    daily_min = try Number.From([Record][summary][min]) otherwise null,
                    daily_max = try Number.From([Record][summary][max]) otherwise null,
                    units = try Text.From([Record][parameter][units]) otherwise null,
                    percent_complete = try Number.From([Record][coverage][percentComplete]) otherwise null
                ]),
                TypedTable = Table.FromRecords(ParsedRows[Row], OutputSchema)
            in
                TypedTable
in
    ResultTable;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "OpenAQ_Daily_Measurements_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "date_utc", DestinationColumnName = "date_utc"], [SourceColumnName = "parameter", DestinationColumnName = "parameter"], [SourceColumnName = "daily_avg", DestinationColumnName = "daily_avg"], [SourceColumnName = "daily_min", DestinationColumnName = "daily_min"], [SourceColumnName = "daily_max", DestinationColumnName = "daily_max"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "percent_complete", DestinationColumnName = "percent_complete"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "location_name", DestinationColumnName = "location_name"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "provider_name", DestinationColumnName = "provider_name"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared OpenAQ_Daily_Measurements = let
    TargetYear = 2016,
    TodayUtc = Date.From(DateTimeZone.UtcNow()),
    DateFrom = #date(TargetYear, 1, 1),
    EndOfYear = #date(TargetYear, 12, 31),
    DateTo = if EndOfYear > TodayUtc then TodayUtc else EndOfYear,

    Sensors = Table.Buffer(ExpandSensorData(TargetYear)),
    SensorRecords = Table.ToRecords(Sensors),

    FetchedList = List.Accumulate(
        SensorRecords,
        {},
        (state, current) =>
            let
                SensorTable = Table.Buffer(FetchSensorData(current[sensor_id], DateFrom, DateTo)),
                RowCount = Table.RowCount(SensorTable)
            in
                if RowCount > 0 then state & { SensorTable } else state
    ),

    EmptySchema = type table [
        sensor_id = text,
        date_utc = date,
        parameter = text,
        daily_avg = number,
        daily_min = number,
        daily_max = number,
        units = text,
        percent_complete = number
    ],
    AllMeasurements = if List.IsEmpty(FetchedList) then #table(EmptySchema, {}) else Table.Combine(FetchedList),

    Joined = Table.NestedJoin(AllMeasurements, {"sensor_id"}, Sensors, {"sensor_id"}, "LocationMeta", JoinKind.LeftOuter),
    ExpandedMeta = Table.ExpandTableColumn(
        Joined,
        "LocationMeta",
        {"location_id", "location_name", "locality", "provider_name", "latitude", "longitude", "Year"}
    ),

    FinalTyped = Table.TransformColumnTypes(ExpandedMeta, {
        {"sensor_id", type text},
        {"date_utc", type date},
        {"parameter", type text},
        {"daily_avg", type number},
        {"daily_min", type number},
        {"daily_max", type number},
        {"units", type text},
        {"percent_complete", type number},
        {"location_id", Int64.Type},
        {"location_name", type text},
        {"locality", type text},
        {"provider_name", type text},
        {"latitude", type number},
        {"longitude", type number},
        {"Year", Int64.Type}
    })
in
    FinalTyped;
shared OpenAQ_Daily_Measurements_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "959bf169-7bd5-4462-8070-0dcab9eb4d7e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ff9da31-76e3-417f-9b47-2616293fec0c"]}[Data],
  Navigation_3 = Navigation_2{[Id = "bronze", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "OpenAQ_Daily_Measurements", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared FetchSensorHourlyData = (sensor_id as text, DateFrom as date, DateTo as date) =>
let
    // Split date range into monthly chunks (max 744 hours/month -> always fits in 1 page, preventing 408 timeouts)
    StartMonth = Date.StartOfMonth(DateFrom),
    EndMonth = Date.StartOfMonth(DateTo),
    MonthStarts = List.Generate(
        () => StartMonth,
        each _ <= EndMonth,
        each Date.AddMonths(_, 1)
    ),

    // Fetch a single monthly window with 1.25s rate pacing + 408/429 recovery
    FetchMonthChunk = (mStart as date) =>
        let
            ChunkStart = if mStart < DateFrom then DateFrom else mStart,
            mEnd = Date.EndOfMonth(mStart),
            ChunkEnd = if mEnd > DateTo then DateTo else mEnd,
            DtFromStr = Date.ToText(ChunkStart, [Format = "yyyy-MM-dd", Culture = "en-US"]) & "T00:00:00Z",
            DtToStr = Date.ToText(ChunkEnd, [Format = "yyyy-MM-dd", Culture = "en-US"]) & "T23:59:59Z",

            MaxRetries = 3,
            Attempts = List.Generate(
                () => [Attempt = 0, Done = false, PrevDone = false, Status = 0, RateLimitHits = 0, Body = null],
                each not [PrevDone] and [Attempt] <= MaxRetries,
                each
                    let
                        NextAttempt = [Attempt] + 1,
                        // 1.25s base delay (~48 req/min); 65s cooldown if previous attempt hit 429; exponential for 408/5xx
                        DelaySeconds =
                            if [Attempt] = 0 then 3.0
                            else if [Status] = 429 then 65
                            else Number.Power(2, [Attempt]) * 2,

                        RawCall = Function.InvokeAfter(
                            () => try Web.Contents(
                                "https://api.openaq.org",
                                [
                                    RelativePath = "v3/sensors/" & Text.From(sensor_id) & "/hours",
                                    Headers = [#"X-API-Key" = ApiKey],
                                    Query = [
                                        #"datetime_from" = DtFromStr,
                                        #"datetime_to" = DtToStr,
                                        #"limit" = "1000",
                                        #"page" = "1"
                                    ],
                                    ManualStatusHandling = {408, 429, 500, 502, 503, 504},
                                    Timeout = #duration(0, 0, 0, 25),
                                    IsRetry = [Attempt] > 0
                                ]
                            ),
                            #duration(0, 0, 0, DelaySeconds)
                        ),

                        // Map client-side socket/read timeouts to 408
                        HttpStatus =
                            if RawCall[HasError] then
                                408
                            else
                                Value.Metadata(RawCall[Value])[Response.Status],

                        NewRateLimitHits = if HttpStatus = 429 then [RateLimitHits] + 1 else [RateLimitHits],

                        // Allow one 65s cooldown retry for 429; hard-stop if 429 persists (hourly 2,000 req cap)
                        CheckedStatus =
                            if NewRateLimitHits > 1 then
                                error Error.Record("HourlyRateLimitHit", "OpenAQ 429 persisted after 65s cooldown (2,000 req/hr quota reached).")
                            else
                                HttpStatus,

                        IsRetryable = List.Contains({408, 429, 500, 502, 503, 504}, CheckedStatus),
                        BufferedBody =
                            if CheckedStatus = 200 and not RawCall[HasError] then
                                Json.Document(Binary.Buffer(RawCall[Value]))
                            else
                                null
                    in
                        [
                            Attempt = NextAttempt,
                            Done = not IsRetryable,
                            PrevDone = [Done],
                            Status = CheckedStatus,
                            RateLimitHits = NewRateLimitHits,
                            Body = BufferedBody
                        ],
                each [Status = [Status], Body = [Body]]
            ),
            LastResp = List.Last(Attempts),
            ChunkRecords =
                if LastResp[Status] = 200 and LastResp[Body] <> null and Record.HasFields(LastResp[Body], "results") then
                    LastResp[Body][results]
                else
                    {}
        in
            ChunkRecords,

    // Force strict sequential month-by-month execution via List.Buffer + List.Count
    AllMonthRecords = List.Accumulate(
        MonthStarts,
        {},
        (state, currentMonth) =>
            let
                BufferedChunk = List.Buffer(FetchMonthChunk(currentMonth)),
                ChunkCount = List.Count(BufferedChunk)
            in
                if ChunkCount > 0 then state & BufferedChunk else state
    ),

    OutputSchema = type table [
        sensor_id = text,
        datetime_utc = datetimezone,
        parameter = text,
        value = number,
        units = text,
        percent_complete = number
    ],

    ResultTable =
        if List.IsEmpty(AllMonthRecords) then
            #table(OutputSchema, {})
        else
            let
                ToTable = Table.FromList(AllMonthRecords, Splitter.SplitByNothing(), {"Record"}, null, ExtraValues.Error),
                ParsedRows = Table.AddColumn(ToTable, "Row", each [
                    sensor_id = Text.From(sensor_id),
                    datetime_utc = try DateTimeZone.From([Record][period][datetimeFrom][utc]) otherwise null,
                    parameter = try Text.From([Record][parameter][name]) otherwise null,
                    value = try Number.From([Record][value]) otherwise null,
                    units = try Text.From([Record][parameter][units]) otherwise null,
                    percent_complete = try Number.From([Record][coverage][percentComplete]) otherwise null
                ]),
                TypedTable = Table.FromRecords(ParsedRows[Row], OutputSchema)
            in
                TypedTable
in
    ResultTable;
