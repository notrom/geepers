'use strict';

var GoogleSpreadsheet = require("google-spreadsheet");
var async = require("async");
var uuid = require('node-uuid');
var semaphore = require('semaphore');

var Geepers = function() {
    var self = this;
    self.spreadSheet = {};
    self.thisSpreadsheetInfo = {};
    self.workSheets = {};
    self.geepersConfig = {};
    
    function setupWorkSheets(spreadsheetInfo, cb) {
        var sheetWorksheets = {};
        async.each(spreadsheetInfo.worksheets, function (sheet, callback) {
            sheetWorksheets[sheet.title] = new Collection(sheet, self.geepersConfig);
            sheetWorksheets[sheet.title].getFields(function(err, fields) {
                callback(err);
            });
        }, function (err) {
            cb(err, sheetWorksheets);
        });
    }
    
    this.collection = function (collectionName) {
        return self.workSheets[collectionName];
    };
    
    this.connect = function (geepersConfig, cb) {
        self.geepersConfig = geepersConfig;
        self.spreadSheet = new GoogleSpreadsheet(geepersConfig.geepers_sheet_id);
        self.spreadSheet.useServiceAccountAuth(geepersConfig, function (err) {
            self.spreadSheet.getInfo(function (err, spreadSheetInfo) {
                self.thisSpreadsheetInfo = spreadSheetInfo;
                setupWorkSheets(spreadSheetInfo, function (err, workSheetsCol) {
                    self.workSheets = workSheetsCol;
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
            cb(err, self.fields);
        });    
    }
    
    this.insertOne = function (obj, opt, cb) {
        obj.gid = uuid.v4();
        // check if the fields in obj exist in worksheet fields
        var missing = fieldsNotInObj2(obj, self.fields);
        if (missing.length !== 0) {
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
    
    this.insertMany = function (objs, cb) {
        var missing = [];
        // process all the objs and see if we need to add any header fields
        async.series([
            //find the missing fields
            function (cb) {
                async.each(objs, function (obj, cb) {
                    obj.gid = 1;
                    var objMissing = fieldsNotInObj2(obj, self.fields);
                    for (var i = 0; i < objMissing.length; i++) {
                        // if the missing array doesn't already have this key, add it
                        if (missing.indexOf(objMissing[i]) === -1) {
                            missing.push(objMissing[i]);
                        }
                    }
                    cb();
                }, function (err) {
                    cb(err);
                });
            }, 
            // insert the missing fileds
            function (cb) {
                if (missing.length !== 0) {
                    // block all others while adding header cells
                    self.sem.take(self.semCap, function () {
                        addHeaderCells(self.workSheet, missing, Object.keys(self.fields).length + 1,
                            function (err) {
                                if (err) {
                                    self.getFields(function (err) {
                                        self.sem.leave(self.semCap);
                                        cb(err);
                                    });
                                } else {
                                    // update the local fields
                                    for (var k = 0; k < missing.length; k++) {
                                        self.fields[missing[k]] = 1;
                                    }
                                    self.sem.leave(self.semCap);
                                    cb(err);
                                }
                            });
                    });
                } else {
                    cb();
                }
            }, 
            // insert all the data
            function (cb) {
                async.each(objs, function (obj, cb) {
                    obj.gid = uuid.v4();
                    self.workSheet.addRow(obj, function (err) {
                        cb(err);
                    });
                }, function (err) {
                    cb(err)
                });
            }],
            function (err, results) {
                cb(err, objs)
            });
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
    
    // find does not return the cursor directly, only in the cb parameter
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

/////// UTILS /////// 

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
    var i = 0;
    async.eachSeries(fields, function (field, cb) {
        addHeaderCell(workSheet, field, startColumn + i,
            function (err) {
                cb(err);
            });
        i++;
    }, 
    function (err) {
        cb(err);
    });
    // insert the missing headers
    /*for (var i = 0; i < fields.length; i++) {
        console.log('i === ', i, fields[i]);
        addHeaderCell(workSheet, fields[i], startColumn+i, 
            function (err) {
                console.log('inside i === ', i, fields[i]);
                if (err) {
                    cb(err);
                } else {
                    if (i === fields.length - 1) {
                        cb();
                    }
                }
            }
        );
    }*/
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



