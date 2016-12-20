namespace NeXL.Quandl
open NeXL.ManagedXll
open NeXL.XlInterop
open System
open System.IO
open System.Runtime.InteropServices
open System.Data
open FSharp.Data
open Newtonsoft.Json
open Newtonsoft.Json.Linq

[<XlQualifiedName(true)>]
module Quandl =

    let private getTableUrl = sprintf "https://www.quandl.com/api/v3/datatables/%s/%s.json"

    let private getSeriesUrl = sprintf "https://www.quandl.com/api/v3/datasets/%s/%s/data.json"

    let private getDbCols (cols : QuandlTableColumn[]) =
        cols |> Array.map (fun col -> 
                               if col.Type = "String" then
                                   new DataColumn(col.Name, typeof<string>)
                               elif col.Type.StartsWith("Date") then
                                   new DataColumn(col.Name, typeof<DateTime>)
                               else
                                   new DataColumn(col.Name, typeof<float>)
                          )

    let private toObj (t : Type) (v : JValue) =
        if v.Type <> JTokenType.Null then
            if t = typeof<string> then
                v.ToObject<string>():>obj
            elif t = typeof<DateTime> then
                v.ToObject<DateTime>():>obj
            else
                v.ToObject<float>():>obj
        else  
            DBNull.Value:>obj

    [<XlFunctionHelp("This function will asynchronously return a Quandl table")>]
    let getTable(
                 [<XlArgHelp("Quandl API Key")>] apiKey : string,
                 [<XlArgHelp("Quandl database code")>] db : string,
                 [<XlArgHelp("Quandl table code")>] table : string,
                 [<XlArgHelp("Selected columns (optional). Comma separated in 1 string or a row/column of strings")>] selColumns : string[] option,
                 [<XlArgHelp("Row or column with row filter names (optional)")>] rowFilterNames : string[] option,
                 [<XlArgHelp("Row or column with row filter values (optional)")>] rowFilterValues : string[] option,
                 [<XlArgHelp("Date format to format output, e.g. 'dd/mm/yyyy' (optional)")>] dateFormat : string option,
                 [<XlArgHelp("True if headers should be returned (optional, default is TRUE)")>] headers : bool option,
                 [<XlArgHelp("True if table should be returned as transposed (optional, default is FALSE)")>] transposed : bool option,
                 [<XlArgHelp("Timestamp to force refresh on recalc. You can use Excel Today() but not Now() (optional)")>] timestamp : DateTime option) =
        async
            {
            let apiPrm = Some ("api_key", apiKey)

            let selColsPrm = 
                match selColumns with 
                    | Some(selCols) when selCols.Length = 0 -> None
                    | Some(selCols) when selCols.Length = 1 -> Some ("qopts.columns", selCols.[0])
                    | Some(selCols) -> Some ("qopts.columns", selCols |> String.concat ",")
                    | None -> None

            let rowFilterPrm =
                match rowFilterNames, rowFilterValues with
                    | Some(filterNames), Some(filterValues) ->
                        filterValues |> Array.zip filterNames |> Array.map Some |> Array.toList
                    | _ -> []

            let dateFormat = defaultArg dateFormat String.Empty

            let transposed = defaultArg transposed false

            let headers = defaultArg headers true

            let query = [apiPrm; selColsPrm] @ rowFilterPrm |> List.choose id

            let! response = Http.AsyncRequest(getTableUrl db table, query, silentHttpErrors = true)
            match response.Body with  
                | Text(json) -> 
                    if response.StatusCode >= 400 then
                        let err = JsonConvert.DeserializeObject<QuandlError>(json)
                        raise (new ArgumentException(err.Quandl_Error.Message))
                        return XlTable.Empty
                    else
                        let table = JsonConvert.DeserializeObject<QuandlTableResponse>(json)
                        let cols = table.DataTable.Columns
                        let data = table.DataTable.Data
                        let cursor = table.Meta.Next_Cursor_Id
                        let dbCols = getDbCols cols
                        let dbTable = new DataTable()
                        dbTable.Columns.AddRange(dbCols)
                        data |> Array.iter (fun r -> 
                                                let row = dbTable.NewRow()
                                                r |> Array.iteri (fun i v ->
                                                                    let t = dbCols.[i].DataType
                                                                    row.[i] <- toObj t v
                                                                 )
                                                dbTable.Rows.Add(row)
                                            )
                        return new XlTable(dbTable, String.Empty, dateFormat, false, transposed, headers)
                | Binary(_) -> 
                    raise (new ArgumentException("Binary response received, json expected"))
                    return XlTable.Empty
            }

    [<XlFunctionHelp("This function will asynchronously return a Quandl dataset (series)")>]
    let getSeries(
                  [<XlArgHelp("Quandl API Key")>] apiKey : string,
                  [<XlArgHelp("Quandl database code")>] db : string,
                  [<XlArgHelp("Quandl dataset (series) code")>] dataset : string,
                  [<XlArgHelp("Series start date (optional)")>] startDate : DateTime option,
                  [<XlArgHelp("Series end date (optional)")>] endDate : DateTime option,
                  [<XlArgHelp("Index of column to be returned (optional)")>] colIndex : int option,
                  [<XlArgHelp("Max number of rows (optional)")>] limit : int option,
                  [<XlArgHelp("Order: asc or desc (optional)")>] order : string option,
                  [<XlArgHelp("Collapse: none, daily, weekly, monthly, quaterly or annual (optional, default is none)")>] collapse : string option,
                  [<XlArgHelp("Transform: none, diff, rdiff, rdiff_from, cumul or normalize (optional, default is none)")>] transform : string option,
                  [<XlArgHelp("Date format to format output, e.g. 'dd/mm/yyyy' (optional)")>] dateFormat : string option,
                  [<XlArgHelp("True if headers should be returned (optional, default is TRUE)")>] headers : bool option,
                  [<XlArgHelp("True if series should be returned as transposed (optional, default is FALSE)")>] transposed : bool option,
                  [<XlArgHelp("Timestamp to force refresh on recalc. You can use Excel Today() but not Now() (optional)")>] timestamp : DateTime option) =
        async
            {
            let apiPrm = Some ("api_key", apiKey)
            
            let startDatePrm = startDate |> Option.map (fun v -> "start_date", v.ToString("yyyy-MM-dd"))

            let endDatePrm = endDate |> Option.map (fun v -> "end_date", v.ToString("yyyy-MM-dd"))

            let limitPrm = limit |> Option.map (fun v -> "limit", v.ToString())

            let colIndexPrm = colIndex |> Option.map (fun v -> "column_index", v.ToString())

            let orderPrm = order |> Option.map (fun v -> "order", v)

            let collapsePrm = collapse |> Option.map (fun v -> "collapse", v)

            let transformPrm = transform |> Option.map (fun v -> "transform", v)

            let transposed = defaultArg transposed false

            let headers = defaultArg headers true

            let dateFormat = 
                match dateFormat with 
                    | Some(dateFormat) -> dateFormat
                    | None -> String.Empty

            let query = [apiPrm; limitPrm; colIndexPrm; orderPrm; collapsePrm; transformPrm;startDatePrm;endDatePrm] |> List.choose id

            let! response = Http.AsyncRequest(getSeriesUrl db dataset, query, silentHttpErrors = true)
            match response.Body with  
                | Text(json) -> 
                    if response.StatusCode >= 400 then
                        let err = JsonConvert.DeserializeObject<QuandlError>(json)
                        raise (new ArgumentException(err.Quandl_Error.Message))
                        return XlTable.Empty
                    else
                        let dataset = JsonConvert.DeserializeObject<QuandlDatasetResponse>(json)
                        let cols = dataset.Dataset_Data.Column_Names
                        let data = dataset.Dataset_Data.Data
                        let dbCols = cols |> Array.mapi (fun i c -> if i = 0 then new DataColumn(c, typeof<DateTime>) else new DataColumn(c, typeof<float>))
                        let dbTable = new DataTable()
                        dbTable.Columns.AddRange(dbCols)
                        data |> Array.iter (fun r -> 
                                                let row = dbTable.NewRow()
                                                r |> Array.iteri (fun i v -> 
                                                                    let t = dbCols.[i].DataType
                                                                    row.[i] <- toObj t v
                                                                 )
                                                dbTable.Rows.Add(row)
                                            )
                        return new XlTable(dbTable, String.Empty, dateFormat, false, transposed, headers)
                | Binary(_) -> 
                    raise (new ArgumentException("Binary response received, json expected"))
                    return XlTable.Empty
            }

    let getErrors(newOnTop: bool) : IEvent<XlTable> =
        UdfErrorHandler.OnError |> Event.scan (fun s e -> e :: s) []
                                |> Event.map (fun errs ->
                                                  let errs = if newOnTop then errs |> List.toArray else errs |> List.rev |> List.toArray
                                                  XlTable.Create(errs, "", "", false, false, true)
                                             )

    [<XlVolatile>]
    [<XlFunctionHelp("Volatile function to get current time with minutes and seconds set to zero.")>]
    let getNowHour() =
        let now = DateTime.Now
        new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0)

    [<XlVolatile>]
    [<XlFunctionHelp("Volatile function to get current time with seconds set to zero.")>]
    let getNowMinute() =
        let now = DateTime.Now
        new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0)

    [<XlVolatile>]
    [<XlFunctionHelp("Volatile function to get current time with seconds rounded by 'roundSec' parameter")>]
    let getNow(
               [<XlArgHelp("Specify accuracy of current time in seconds. Optional, default is 15")>] roundSec : int option) =
        let moduloSec = defaultArg roundSec 15
        let now = DateTime.Now
        let s = now.Second
        new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, (s / moduloSec) * moduloSec)
        
