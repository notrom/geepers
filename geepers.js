var Mongish = require("./Mongish");
var GoogleSpreadsheet = require("google-spreadsheet");

var creds = require('./gsheetsauth.json');

var spreadSheet = {};
var thisSpreadsheetInfo = {};
var workSheets = {};

function setupWorkSheets(spreadsheetInfo, cb) {
    var sheetWorksheets = {};
    var c = spreadsheetInfo.worksheets.length;
    for (var i in spreadsheetInfo.worksheets) {
        sheetWorksheets[spreadsheetInfo.worksheets[i].title] = 
                    new Mongish(spreadsheetInfo.worksheets[i], creds);
        sheetWorksheets[spreadsheetInfo.worksheets[i].title].getFields(function (fields) {
            // TODO: counting callbacks? Should probably be promises ...
            if (--c === 0) {
                cb(sheetWorksheets);
            }
        });
    }
    //return sheetWorksheets;
}

exports.collection = function (collectionName) {
    return workSheets[collectionName];
};

exports.connect = function (gsId, cb) {
    spreadSheet = new GoogleSpreadsheet(gsId);
    spreadSheet.useServiceAccountAuth(creds, function(err){
        spreadSheet.getInfo( function (err, spreadSheetInfo) {
            thisSpreadsheetInfo = spreadSheetInfo;
            //console.log( spreadSheetInfo.title + ' is loaded' );
            setupWorkSheets(spreadSheetInfo, function (workSheetsCol) {
                workSheets = workSheetsCol;
                // TODO: not sure if this is correct ideal ...
                cb(null, exports);
            });
        });
    });
};

