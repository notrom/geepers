'use strict';

var GoogleSpreadsheet = require("google-spreadsheet");
var async = require("async");
var uuid = require('node-uuid');
var creds = require('./gsheetsauth.json');
var semaphore = require('semaphore');

var Geepers = function() {
    var self = this;
    var spreadSheet = {};
    var thisSpreadsheetInfo = {};
    var workSheets = {};
    
    function setupWorkSheets(spreadsheetInfo, cb) {
        var sheetWorksheets = {};
        async.each(spreadsheetInfo.worksheets, function (sheet, callback) {
            sheetWorksheets[sheet.title] = new Collection(sheet, creds);
            sheetWorksheets[sheet.title].getFields(function(err, fields) {
                callback(err);
            });
        }, function (err) {
            cb(err, sheetWorksheets);
        });
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
                setupWorkSheets(spreadSheetInfo, function (err, workSheetsCol) {
                    workSheets = workSheetsCol;
                    // TODO: not sure if this is correct or ideal ...
                    cb(err, self);
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
    self.fields = {gid:1};
    self.semCap = 10;
    self.sem = semaphore(self.semCap);

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
            cb(err, self.fields);
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
    
    this.update = function (filter, updates, cb) {
        // find rows
        var query = filterToGsQuery(filter);
        // use of sem to lock during some other ops
        self.sem.take(1, function () {
            findRows(self.workSheet, query, function (err, rowData) {
                self.sem.leave(1);
                if (!err) {
                    async.each(rowData, function(row, cb) {
                        // update props
                        for (var prop in updates) {
                            if (updates.hasOwnProperty(prop)) {
                                row[prop] = updates[prop];
                            }
                        }
                        // save the row
                        row.save(function (err) {
                            cb(err);
                        });
                    }, function (err) {
                        cb(err);
                    });
                } else {
                    cb(err);
                }
            });
        });
    }
    
    this.deleteMany = function (filter, cb) {
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
    
    this.find = function (filter, projection, cb) {
        var resCursor;
        // if only 2 params, then 2 is cb, projection is empty (so all)
        if (!cb && typeof projection === 'function') {
            cb = projection;
            projection = undefined;
        } 
        var query = filterToGsQuery(filter);
        // use of sem to lock during some other ops
        self.sem.take(1, function () {
            findRows(self.workSheet, query, function (err, rowData) {
                self.sem.leave(1);
                // based on what we know about the fields in this sheet
                var data = [];
                var obj = {};
                var projectedFields = projectionIncludeFields(self.fields, projection);
                for (var i = 0; !err && i < rowData.length; i++) {
                    obj = {};
                    for (var prop in projectedFields) {
                        if (projectedFields.hasOwnProperty(prop)) {
                            obj[prop] = rowData[i][prop];
                        }
                    }
                    data.push(obj);
                }
                resCursor = new Cursor(data);
                cb(err, resCursor);
            });
        });
    };
    
    // Returns the query string generated for the provided filter object
    this.filterQuery = function (filter) {
        return filterToGsQuery(filter);
    }
    
    // Returns the fields returned when using a given projection
    this.projectionFields = function (projection) {
        return projectionIncludeFields(self.fields, projection);  
    }
}

function Cursor (dataIn) {
    var self = this;
    self.dataArray = dataIn;
    self.currentIndex = 0;
    
    this.toArray = function () {
        return self.dataArray;       
    }
    
    // only handles a single sort spec.
    this.sort = function(sortSpec) {
        var desc = false;
        for (var prop in sortSpec) {
            if (sortSpec.hasOwnProperty(prop)) {
                if (sortSpec[prop] === -1) desc = true;
                self.dataArray = self.dataArray.sort(sort_by(prop, desc));
                break;
            }
        }
        return self;
    }
    
    this.count = function() {
        return self.dataArray.length;
    }
    
    this.forEach = function (fnc) {
        async.each(self.dataArray, function (row, cb) {
            fnc(row);
        }, function (err) {
            return;
        });
    } 
    
    this.hasNext = function() {
        if (self.currentIndex < self.dataArray.length) {
            return true;
        } else {
            return false;
        }
    }
    
    this.next = function() {
        var result = {};
        if (self.currentIndex < self.dataArray.length) {
            result = self.dataArray[self.currentIndex];
            self.currentIndex++;
        } else {
            result = null;
        }
        return result;
    }
    
    this.map = function (fnc) {
        var mapResults = [];
        for (var i = 0; i < self.dataArray.length; i++) {
            mapResults.push(fnc(self.dataArray[i]));
        }
        return mapResults;
    }
}

module.exports = Geepers;

// from http://stackoverflow.com/questions/979256/sorting-an-array-of-javascript-objects/979325#979325
var sort_by = function(field, reverse, primer){

   var key = primer ? 
       function(x) {return primer(x[field])} : 
       function(x) {return x[field]};

   reverse = !reverse ? 1 : -1;

   return function (a, b) {
       return a = key(a), b = key(b), reverse * ((a > b) - (b > a));
     } 
}

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
            } else if (ele === '$gte') {
                queryPart += ' >= ' + filterObj[ele];
            } else if (ele === '$lt') {
                queryPart += ' < ' + filterObj[ele];
            } else if (ele === '$lte') {
                queryPart += ' <= ' + filterObj[ele];
            } else if (ele === '$ne') {
                queryPart += ' <> ' + filterObj[ele];
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

function projectionIncludeFields(fields, projection) {
    var included = {};
    var projInclusive = true;
    // only process projections that are not null and have properties
    if (projection && Object.keys(projection).length > 0) {
        // Determie if this is an inclusive projection or exclusive
        // based on the first element that's not the gid
        if (Object.keys(projection).length === 1 && 
            projection.hasOwnProperty('gid')) {
            included = JSON.parse(JSON.stringify(fields));
        } else {
            for (var pEle in projection) {
                if (projection.hasOwnProperty(pEle) && pEle !== 'gid') {
                    projInclusive = projection[pEle];
                    if (!projInclusive) {
                        included = JSON.parse(JSON.stringify(fields));
                    }
                    break;
                }
            }
        }
        // for all projections properties that exist in fields
        for (var fEle in fields) {
            if (projection.hasOwnProperty(fEle) &&
                fields.hasOwnProperty(fEle)) {
                if (projInclusive) {
                    included[fEle] = 1;
                } else {
                    delete included[fEle];
                }
            }
        }
        // gid is always returned unless explicitly excluded
        if (projection.hasOwnProperty('gid')) {
            if (!projection['gid']) {
                delete included['gid'];
            }
        } else {
            included['gid'] = 1;
        }
    } else {
        included = JSON.parse(JSON.stringify(fields));
    }
    return included;
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



