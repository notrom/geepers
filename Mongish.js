
//var GoogleSpreadsheet = require("google-spreadsheet");
var async = require("async");
var uuid = require('node-uuid');
//var semCap = 10;
//var sem = require('semaphore')(semCap);


// Constructor
function Mongish(workSheet, creds) {
    // always initialize all instance properties
    this.workSheet = workSheet;
    this.creds = creds;
    this.fields = {};
    this.semCap = 10;
    this.sem = require('semaphore')(this.semCap);
}

Mongish.prototype.getFields = function (cb) {
    var self = this;
    this.fields = {};
    this.workSheet.getCells({   "min-row":1, "max-row":1, 
                                "min-col":1, "max-col":this.workSheet.colCount}, 
                             function (err, cells) {
        var i;
        for (i=0; i < cells.length; i++) {
            self.fields[cells[i].value] = 1;
        }
        //console.log(JSON.stringify(self.fields));
        cb(self.fields);
    });    
}

Mongish.prototype.insertOne = function(obj, opt, cb) {
    var self = this;
    obj.gid = uuid.v4();
    //console.log(obj.gid);
    // check if the fields in obj exist in worksheet fields
    var missing = fieldsNotInObj2(obj, self.fields);
    
    if (missing.length !== 0) {
        //console.log("Need to add new header columns for " + missing);
        // block all others while adding header cells
        self.sem.take(self.semCap, function() {
            addHeaderCells(self.workSheet, missing, Object.keys(self.fields).length+1,
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

Mongish.prototype.insertMany = function(objs, opt, cb) {
    var self = this;
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
                self.sem.take(self.semCap, function() {
                    addHeaderCells(self.workSheet, missing, Object.keys(self.fields).length+1,
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

Mongish.prototype.deleteMany = function(filter, opt, cb) {
    var self = this;
    var query = filterToGsQuery(filter);
    
    // while deleting, block all others
    self.sem.take(self.semCap, function() {
        findRows(self.workSheet, query, function (err, rowData) {
            // reverse the order of the resulting rows, work from the bottom up
            rowData.reverse();
            async.eachSeries( rowData, function(row, cb) {
                deleteRow(row, function (err, result) {
                    cb(err);
                });
            }, function(err) {
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

Mongish.prototype.find = function(filter, opt, cb) {
    var self = this;
    var query = filterToGsQuery(filter);
    // use of sem to lock during some other ops
    self.sem.take(1, function() {
        findRows(self.workSheet, query, function (err, rowData) {
            // based on what we know about the fields in this sheet
            var data = [];
            self.sem.leave(1);
            for (var i = 0; i < rowData.length; i++) {
                var obj = {};
                for (var prop in self.fields) {
                    if(self.fields.hasOwnProperty(prop)) {
                        obj[prop] = rowData[i][prop];
                    }
                }
                data.push(obj);
            }
            cb(err, data);
        });
    });
};

// export the class
module.exports = Mongish;

function filterToGsQuery (filter) {
    return filterOptProcess("", filter, " and ", true);
}

//console.log(filterToGsQuery({i:1, time: {$gt: 10}, $or:[{i:3}, {i:0}, {time: {$gt: 0}} ]}));
//console.log(filterToGsQuery({$or: [{i:1, time: {$gt: 10}, $or:[{i:3}, {i:0}, {time: {$gt: 0}} ]}, {i:99}]}));

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