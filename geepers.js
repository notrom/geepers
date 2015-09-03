'use strict';

var GoogleSpreadsheet = require("google-spreadsheet");
var async = require("async");
var uuid = require('node-uuid');
var creds = require('./gsheetsauth.json');

var Geepers = function() {
    var self = this;
    var spreadSheet = {};
    var thisSpreadsheetInfo = {};
    var workSheets = {};
    
    function setupWorkSheets(spreadsheetInfo, cb) {
        var sheetWorksheets = {};
        var c = spreadsheetInfo.worksheets.length;
        for (var i in spreadsheetInfo.worksheets) {
            sheetWorksheets[spreadsheetInfo.worksheets[i].title] =
            new Collection(spreadsheetInfo.worksheets[i], creds);
            sheetWorksheets[spreadsheetInfo.worksheets[i].title].getFields(function (fields) {
                // TODO: counting callbacks? Should probably be promises ...
                if (--c === 0) {
                    cb(sheetWorksheets);
                }
            });
        }
        //return sheetWorksheets;
    }
    
    this.collection = function (collectionName) {
        return workSheets[collectionName];
    };
    
    this.connect = function (gsId, cb) {
        spreadSheet = new GoogleSpreadsheet(gsId);
        spreadSheet.useServiceAccountAuth(creds, function (err) {
            spreadSheet.getInfo(function (err, spreadSheetInfo) {
                thisSpreadsheetInfo = spreadSheetInfo;
                //console.log( spreadSheetInfo.title + ' is loaded' );
                setupWorkSheets(spreadSheetInfo, function (workSheetsCol) {
                    workSheets = workSheetsCol;
                    // TODO: not sure if this is correct ideal ...
                    cb(null, self);
                });
            });
        });
    };
};

// Constructor
function Collection(workSheet, creds) {
    var self = this;
    self.workSheet = workSheet;
    self.creds = creds;
    self.fields = {};
    self.semCap = 10;
    self.sem = require('semaphore')(self.semCap);

    this.getFields = function (cb) {
        self.fields = {};
        self.workSheet.getCells({   "min-row":1, "max-row":1, 
                                "min-col":1, "max-col":self.workSheet.colCount}, 
                             function (err, cells) {
            var i;
            for (i=0; i < cells.length; i++) {
                self.fields[cells[i].value] = 1;
            }
            //console.log(JSON.stringify(self.fields));
            cb(self.fields);
        });    
    }
    
    this.insertOne = function (obj, opt, cb) {
        obj.gid = uuid.v4();
        //console.log(obj.gid);
        // check if the fields in obj exist in worksheet fields
        var missing = fieldsNotInObj2(obj, self.fields);

        if (missing.length !== 0) {
            //console.log("Need to add new header columns for " + missing);
            // block all others while adding header cells
            self.sem.take(self.semCap, function () {
                addHeaderCells(self.workSheet, missing, Object.keys(self.fields).length + 1,
                    function (err) {
                        if (err) {
                            self.getFields(function (err2) {
                                self.sem.leave(self.semCap);
                                cb(err, null);
                            });
                        } else {
                            // update the local fields
                            var i;
                            for (i = 0; i < missing.length; i++) {
                                self.fields[missing[i]] = 1;
                            }
                            self.sem.leave(self.semCap);
                            // write the object
                            self.workSheet.addRow(obj, function (err) {
                                cb(err, obj);
                            });
                        }
                    }
                    );
            });
        } else {
            // write the object
            self.workSheet.addRow(obj, function (err) {
                cb(err, obj);
            });
        }
    };
    
    this.insertMany = function (objs, opt, cb) {
        var overallErr = null;
        for (var i = 0; i < objs.length; i++) {
            (function () {
                var j = i;
                objs[j].gid = uuid.v4();
                var obj = objs[j];
                //console.log(obj.gid);
                //console.log(obj);
                // check if the fields in obj exist in worksheet fields
                var missing = fieldsNotInObj2(obj, self.fields);

                if (missing.length !== 0) {
                    //console.log("Need to add new header columns for " + missing);
                    // block all others while adding header cells
                    self.sem.take(self.semCap, function () {
                        addHeaderCells(self.workSheet, missing, Object.keys(self.fields).length + 1,
                            function (err) {
                                if (err) {
                                    self.getFields(function (err2) {
                                        self.sem.leave(self.semCap);
                                        cb(err, null);
                                    });
                                } else {
                                    // update the local fields
                                    var k;
                                    for (k = 0; k < missing.length; k++) {
                                        self.fields[missing[k]] = 1;
                                    }
                                    self.sem.leave(self.semCap);
                                    // write the object
                                    self.workSheet.addRow(obj, function (err) {
                                        if (err) {
                                            overallErr = err;
                                        }
                                        if (j >= objs.length - 1) {
                                            cb(overallErr, objs);
                                        }
                                    });
                                }
                            }
                            );
                    });
                } else {
                    // write the object
                    self.workSheet.addRow(obj, function (err) {
                        if (err) {
                            overallErr = err;
                        }
                        if (j >= objs.length - 1) {
                            cb(overallErr, objs);
                        }
                    });
                }
            })();
        }
    };
    
    this.deleteMany = function (filter, opt, cb) {
        var query = filterToGsQuery(filter);
    
        // while deleting, block all others
        self.sem.take(self.semCap, function () {
            findRows(self.workSheet, query, function (err, rowData) {
                // reverse the order of the resulting rows, work from the bottom up
                rowData.reverse();
                async.eachSeries(rowData, function (row, cb) {
                    deleteRow(row, function (err, result) {
                        cb(err);
                    });
                }, function (err) {
                    self.sem.leave(self.semCap);
                    if (err) {
                        cb(err, false);
                    } else {
                        cb(err, true);
                    }
                });
            });
        });
    };
    
    this.find = function (filter, opt, cb) {
        var query = filterToGsQuery(filter);
        // use of sem to lock during some other ops
        self.sem.take(1, function () {
            findRows(self.workSheet, query, function (err, rowData) {
                self.sem.leave(1);
                // based on what we know about the fields in this sheet
                var data = [];
                for (var i = 0; !err && i < rowData.length; i++) {
                    var obj = {};
                    for (var prop in self.fields) {
                        if (self.fields.hasOwnProperty(prop)) {
                            obj[prop] = rowData[i][prop];
                        }
                    }
                    data.push(obj);
                }
                cb(err, data);
            });
        });
    };
}



module.exports = Geepers;



// UTILS
function filterToGsQuery (filter) {
    return filterOptProcess("", filter, " and ", true);
}

// GS queries - https://developers.google.com/google-apps/spreadsheets/#sending_a_structured_query_for_rows
//              - "age > 25 and height < 175"
// Mongo filter - { "equals": "this",
//                  "greaterThan": { $gt: 30 },
//                  $or: [ { "cuisine": "Italian" }, { "address.zipcode": "10075" } ],
//                  "one": "these are", "two": "anded"}
function filterOptProcess(queryPart, filterObj, op, first) {
    var localOp = op;
    for (var ele in filterObj) {
        if (first) {
            localOp = op;
            op = " ";
            first = false;
        } else {
            op = localOp;
        }
        if(filterObj.hasOwnProperty(ele)) {
            if (typeof filterObj[ele] === 'object' && ele === '$or') {
                queryPart += op + ' ( ';
                var orFirst = true;
                for (var i = 0; i < filterObj['$or'].length; i++) {
                    queryPart = filterOptProcess(queryPart, filterObj['$or'][i], ' or ', orFirst);
                    orFirst = false;
                }
                queryPart += ' ) ';                
            } else if (typeof filterObj[ele] === 'object') {
                queryPart = filterOptProcess(queryPart + op + ele, filterObj[ele], op, false);
            } else if (ele === '$gt') {
                queryPart += ' > ' + filterObj[ele];
            } else if (ele === '$lt') {
                queryPart += ' < ' + filterObj[ele];
            } else {
                if (typeof filterObj[ele] === 'string') {
                    filterObj[ele] = '"' + filterObj[ele] + '"';
                }
                queryPart += op + ele + ' = ' + filterObj[ele];
            }
        }
    }
    return queryPart;
}

function deleteRow(row, cb) {
    row.del(function (err, result) {
        cb(err, result);
    });
}

function saveRow(row, cb) {
    row.save(function (err, result) {
        cb(err, result);
    });
}

function findRows(workSheet, query, cb) {
    workSheet.getRows({query: query}, function (err, rowData) {
        if (err) err += ' - ['+query+']'; 
        cb(err, rowData);
    });
}

function addRow (workSheet, obj, cb) {
    workSheet.addRow(obj, function (err) {
        cb(err);
    });
}

function rowToObj (row, fields) {
    var rowObj = {};
    var objFields = Object.keys(fields);
    for (var i = 0; i < objFields.length; i++) {
        rowObj[objFields[i]] = row[objFields[i]];
    }
    return rowObj;
}

function fieldsNotInObj2 (obj1, obj2) {
    var missing = [];
    for (var field in obj1) {
        if (obj1.hasOwnProperty(field)) {
            if (!obj2.hasOwnProperty(field)) {
                missing.push(field);
            }
        }
    }
    return missing;
}

function addHeaderCells (workSheet, fields, startColumn, cb) {
    // insert the missing headers
    var i;
    for (i = 0; i < fields.length; i++) {
        addHeaderCell(workSheet, fields[i], startColumn+i, 
            function (err) {
                if (err) {
                    cb(err);
                } else {
                    if (i === fields.length - 1) {
                        cb();
                    }
                }
            }
        );
    }
}

function addHeaderCell (workSheet, fieldName, columnNum, cb) {
    workSheet.getCells({   "min-row":1, "max-row":1, 
                           "min-col":columnNum, "max-col":columnNum,
                           "return-empty":true }, 
        function (err, cells) {
            if (err) {
                cb(err);
            } else {
                cells[0].setValue(fieldName, function () {
                    cb();
                });
            }
        });
}



