
var assert = require("assert");

// My modules
var geepers = require('../geepers.js');
var geepersId = "1NDpeh2QcZiadjzhIqW9e33IsEpmiM7XMafB2LJueA5c";
var mochaTestSheet = 'functionTests';
//geepers.connect(geepersId, tests);

describe('geepers', function() {
    var db = {};
    var testData = [{i:20, time:202020, word:'this is 20'},
                    {i:21, time:212121, word:'this is 21'},
                    {i:22, time:222222, word:'this is 22'},
                    {i:23, time:232323, word:'this is 23'},
                    {i:24, time:242424, word:'this is 24'},
                    {i:25, time:252525, word:'this is 25'},
                    {i:26, time:262626, word:'this is 26'},
                    {i:27, time:272727, word:'this is 27'},
                    {i:28, time:282828, word:'this is 28'},
                    {i:28, time:292929, word:'this is 28'}];
    
    describe('#collection()', function () {
        beforeEach(function (done) {
            this.timeout(30000);
            geepers.connect(geepersId, function (err, dbConn) {
                db = dbConn;
                done(err);
            });
        });
        it('Returns an object ', function(done) {
            this.timeout(5000);
            var mongish = db.collection(mochaTestSheet);
            if (mongish) done();
        });
    });
    describe.only('#Mongish.find()', function () {
        before(function (done) {
            this.timeout(30000);
            geepers.connect(geepersId, function (err, dbConn) {
                if (err) throw err;
                db = dbConn;
                db.collection(mochaTestSheet).deleteMany({}, {}, function (err, result) {
                    db.collection(mochaTestSheet).insertMany(testData, {}, function (err, result) {
                        done(err);
                    });
                });
            });
        });
        it('with empty filter, returns all records', function (done) {
            db.collection(mochaTestSheet).find({},{},function (err, result) {
                assert.equal(testData.length, result.length);
                done(err);
            });
        });
        it('equals 22 filter, returns only 1 record, with ', function (done) {
            db.collection(mochaTestSheet).find({i:22},{},function (err, result) {
                assert.equal(1, result.length);
                assert.equal(22, result[0].i);
                done(err);
            });
        });
        it('Greater than 22 filter returns only those greater, 7 records', function (done) { 
            db.collection(mochaTestSheet).find({i:{$gt:22}},{},function (err, result) {
                assert.equal(7, result.length);
                done(err);
            });
        });
        it('Less than 22 filter returns only those less than, 2 records', function (done) { 
            db.collection(mochaTestSheet).find({i:{$lt:22}},{},function (err, result) {
                assert.equal(2, result.length);
                done(err);
            });
        });
        it('or filter 22 or 23 return only those 2 records', function (done) { 
            db.collection(mochaTestSheet).find({$or:[{i:22},{i:23}]},{},function (err, result) {
                assert.equal(2, result.length);
                assert.equal(true, (result[0].i == 22 || result[1].i == 22));
                assert.equal(true, (result[0].i == 23 || result[1].i == 23));
                done(err);
            });
        });
        it('equals filter "i = 28", returns 2 records, correct time values ', function (done) {
            db.collection(mochaTestSheet).find({i:28},{},function (err, result) {
                assert.equal(2, result.length);
                assert.equal(28, result[0].i);
                assert.equal(282828, result[0].time);
                assert.equal(28, result[1].i);
                assert.equal(292929, result[1].time);
                done(err);
            });
        });
        it('and filter "i == 28 and time == 282828" returns 1 records', function (done) {
            db.collection(mochaTestSheet).find({i:28,time:282828},{},function (err, result) {
                assert.equal(1, result.length);
                assert.equal(282828, result[0].time);
                assert.equal(28, result[0].i);
                done(err);
            });
        });
        it('and filter on string property "word == "this is 20" returns 1 records', function (done) {
            db.collection(mochaTestSheet).find({word:'this is 20'},{},function (err, result) {
                assert.equal(1, result.length);
                assert.equal(202020, result[0].time);
                assert.equal(20, result[0].i);
                done(err);
            });
        });
    });
    describe('#Mongish.deleteMany()', function () {
        beforeEach(function (done) {
            this.timeout(30000);
            geepers.connect(geepersId, function (err, dbConn) {
                if (err) throw err;
                db = dbConn;
                db.collection(mochaTestSheet).deleteMany({}, {}, function (err, result) {
                    db.collection(mochaTestSheet).insertMany(testData, {}, function (err, result) {
                        done(err);
                    });
                });
            });
        });
        // deletes are sloooowwww
        this.timeout(30000);
        it('No err', function (done) {
            db.collection(mochaTestSheet).deleteMany({i:20},{},function (err, result) {
                done(err);
            });
        });
        it('result is true', function (done) {
            db.collection(mochaTestSheet).deleteMany({i:20},{},function (err, result) {
                if (result) done(err);
            });
        });
        it('delete by filter with one match (i == 20) reduces count by 1, removes expected', function (done) {
            db.collection(mochaTestSheet).deleteMany({i:20},{},function (err, result) {
                db.collection(mochaTestSheet).find({},{},function (err, results) {
                    assert.equal(testData.length - 1, results.length);
                    db.collection(mochaTestSheet).find({i:20},{},function (err, results) {
                        assert.equal(0, results.length);
                        done();
                    });
                });
            });
        });
        it('delete by filter > 26 reduces count by 3, and removes expected', function (done) {
            db.collection(mochaTestSheet).deleteMany({i: {$gt: 26}},{},function (err, result) {
                db.collection(mochaTestSheet).find({},{},function (err, results) {
                    assert.equal(testData.length - 3, results.length);
                    db.collection(mochaTestSheet).find({i: {$gt: 26}},{},function (err, results) {
                        assert.equal(0, results.length);
                        done();
                    });
                });
            });
        });
    });
});

//describe('geepers.Mongish', function () {
//    geepers.connect(geepersId, function (err, db) {
//        mongish = db.collection('functionTests');
//        describe('#deleteMany()', function () {
//            it('No err result is true', function (done) {
//                mongish.deleteMany({},{},function (err, result) {
//                   done(err);
//                });
//            });
//        });
    //});
//});

function tests (err, db) {
    
    async.series([
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").deleteMany({},{},function (err, result) { 
                    console.log("deleteAll", (new Date()) - sdts);
                    callback(err, {test: "deleteAll", expected: true, actual: result});
                });
            },
            function(callback) {
                var insObjs = createNObjects(10);
                
                var sdts = new Date();
                db.collection("functionTests").insertMany(insObjs,{},function (err, result) {
                    console.log("insertFirst10", (new Date()) - sdts);
                    callback(err, {test: "insertFirst10", expected: 10, actual: result.length});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").find({},{},function (err, result) {
                    console.log("findAllEquals10", (new Date()) - sdts);
                    callback(err, {test: "findAllEquals10", expected: 10, actual: result.length});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").find({i:7},{},function (err, result) {
                    console.log("findI7Equals1", (new Date()) - sdts);
                    callback(err, {test: "findI7Equals1", expected: 1, actual: result.length});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").insertOne({i: 7, time: 1111 }, {}, function (err, result) {
                    console.log("insertI7Time1111", (new Date()) - sdts);
                    callback(err, {test: "insertI7Time1111", expected: 1111, actual: result.time});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").find({i:7},{},function (err, result) {
                    console.log("findI7Equals2", (new Date()) - sdts);
                    callback(err, {test: "findI7Equals2", expected: 2, actual: result.length});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").find({i:7},{},function (err, result) {
                    console.log("findI7SecondTime1111", (new Date()) - sdts);
                    callback(err, {test: "findI7SecondTime1111", expected: '1111', actual: result[1].time});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").find({i: {$gt: 1}},{},function (err, result) {
                    console.log("findIgreaterThan1", (new Date()) - sdts);
                    console.log(result);
                    callback(err, {test: "findIgreaterThan1", expected: 9, actual: result.length});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").insertMany([{i:20, time:202020},
                                                    {i:21, time:212121}],{},function (err, result) {
                    console.log("insertMany20And21", (new Date()) - sdts);
                    callback(err, {test: "insertMany20And21", expected: 2, actual: result.length});
                });
            },
            function(callback) {
                var sdts = new Date();
                db.collection("functionTests").deleteMany({},{},function (err, result) { 
                    console.log("deleteAll2", (new Date()) - sdts);
                    callback(err, {test: "deleteAll2", expected: true, actual: result});
                });
            }
        ],
        // optional callback
        function(err, results){
            if (err) {
                console.log(err);
            }
            for (var i = 0; i < results.length; i++) {
                console.log(results[i]);
                if (typeof results.expected === 'object') {
                    if (!assert.deepEqual(actual, expected)) {
                        console.log("TEST FAILED: " + results[i].test);
                    }
                } else {
                    if (results[i].expected !== results[i].actual) {
                        console.log("TEST FAILED: " + results[i].test);
                    }
                }
            }
        });
}

function createNObjects(n) {
    var objs = [];
    for (var i = 0; i < n; i ++) {
        objs.push({i: i, time: new Date(), istring: '' + i + ' is ' + i});
    }
    return objs;
}
