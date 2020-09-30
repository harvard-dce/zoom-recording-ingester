/*
 * script to export data of the named sheet as a post request to an endpoint
 * Adapted from https://gist.github.com/Ryahn/8a2a9ef241731031c998a51eadabc9ec
 */

// Put the name of the sheet (tab) here
var sheetName = "";
// Put the ZIP schdule update endpoint url here
var scheduleUpdateEndpoint = "";

var delimiter = ",";

function onOpen() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var csvMenuEntries = [
    { name: "Update ZIP Schedule", functionName: "sendScheduleCsv" },
  ];
  ss.addMenu("ZIP", csvMenuEntries);
}

function sendScheduleCsv() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName("ZIP");
  // convert all available sheet data to csv format
  var csvData = convertRangeToCsvFile_(sheet);
  // create a file in the Docs List with the given name and the csv data
  var requestParams = {
    method: "post",
    contentType: "text/csv",
    payload: csvData,
  };
  var resp = UrlFetchApp.fetch(scheduleUpdateEndpoint, requestParams);
  // need to show some kind of confirmation here
  ss.toast(resp.getContentText(), "Schedule Updated", 3);
}

function convertRangeToCsvFile_(sheet) {
  // get available data range in the spreadsheet
  var activeRange = sheet.getDataRange();
  try {
    var data = activeRange.getValues();
    var csvFile = undefined;

    // loop through the data in the range and build a string with the csv data
    if (data.length > 1) {
      var csv = "";
      for (var row = 0; row < data.length; row++) {
        for (var col = 0; col < data[row].length; col++) {
          if (data[row][col].toString().indexOf(",") != -1) {
            data[row][col] = '"' + data[row][col] + '"';
          }
        }

        // join each row's columns
        // add a carriage return to end of each row, except for the last one
        if (row < data.length - 1) {
          csv += data[row].join(delimiter) + "\r\n";
        } else {
          csv += data[row];
        }
      }
      csvFile = csv;
    }
    return csvFile;
  } catch (err) {
    Logger.log(err);
    Browser.msgBox(err);
  }
}
