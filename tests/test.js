
var assert = require("assert");

// My modules
var Geepers = require('../geepers.js');
var geepersId = "1NDpeh2QcZiadjzhIqW9e33IsEpmiM7XMafB2LJueA5c";
var mochaTestSheet = 'functionTests';
//geepers.connect(geepersId, tests);

var geepers = new Geepers();

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
            var collection = db.collection(mochaTestSheet);
            if (collection) done();
        });
    });
    describe('#Collection.filter()', function () {
        it('and');
        it('$or');
        it('$gt');
        it('$lt');
        it('not?');
        it('<=?');
        it('>=?');
        it('Other filter types?');
    });
    describe('#Collection.find()', function () {
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
                assert.equal(err, null);
                assert.notEqual(result, null);
                assert.equal(1, result.length);
                assert.equal(202020, result[0].time);
                assert.equal(20, result[0].i);
                done(err);
            });
        });
        it('sort order');
        it('selective fields');
    });
    describe('#Collection.insertMany()', function () {
        beforeEach(function (done) {
            this.timeout(30000);
            geepers.connect(geepersId, function (err, dbConn) {
                if (err) throw err;
                db = dbConn;
                db.collection(mochaTestSheet).deleteMany({}, {}, function (err, result) {
                    done(err);
                });
            });
        });
       it('partial object (missing properties) insert ok', function (done) {
           db.collection(mochaTestSheet).insertMany([{i:1}], {}, function (err, result) {
               assert.equal(1, result.length);
               assert.equal(1, result[0].i);
               assert.equal(null, result[0].time);
               done(err);
           });
        }); 
        it('gid property created and applied, can be queried', function (done) {
           db.collection(mochaTestSheet).insertMany([{i:1},{i:2}], {}, function (err, result) {
               assert.equal(result.length, 2);
               assert.equal((result[0].i == 1 || result[1].i == 1), true);
               assert.equal((result[0].i == 2 || result[1].i == 2), true);
               assert.notEqual(result[0].gid, null);
               assert.notEqual(result[1].gid, null);
               var foundI = result[0].i;
               var foundGid = result[0].gid;
               db.collection(mochaTestSheet).find({gid:foundGid}, {}, function (err, results) {
                   assert.equal(results.length, 1);
                   assert.equal(results[0].i, foundI);
                   done(err);
               });
           });
        });
    });
    describe('#Collection.deleteMany()', function () {
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

