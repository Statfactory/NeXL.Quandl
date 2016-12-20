namespace NeXL.Quandl
open NeXL.ManagedXll
open NeXL.XlInterop
open System
open Newtonsoft.Json
open Newtonsoft.Json.Linq

[<XlInvisible>]
type QuandlErrorMsg =
    {
     Code : string
     Message : string
    }

[<XlInvisible>]
type QuandlError =
    {
        Quandl_Error : QuandlErrorMsg
    }

[<XlInvisible>]
type QuandlTableColumn =
    {
     Name : string
     Type : string
    }

[<XlInvisible>]
type QuandlTable =
    {
     Data : JValue[][]
     Columns : QuandlTableColumn[]
    }

[<XlInvisible>]
type QuandlCursor =
    {
     Next_Cursor_Id : string
    }

[<XlInvisible>]
type QuandlTableResponse =
    {
     DataTable : QuandlTable
     Meta : QuandlCursor
    }

[<XlInvisible>]
type QuandlDataset =
    {
     Data : JValue[][]
     Start_Date : DateTime
     End_Date : DateTime
     Frequency : string
     Limit : Nullable<int>
     Transform : string
     Column_Index : Nullable<int>
     Column_Names : string[]
     Collapse : string
     Order : string
    }

[<XlInvisible>]
type QuandlDatasetResponse =
    {
     Dataset_Data : QuandlDataset
    }


    
  