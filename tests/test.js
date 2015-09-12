'use strict';

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
    describe('#Collection.filterQuery()', function () {
        before(function (done) {
            this.timeout(30000);
            geepers.connect(geepersId, function (err, dbConn) {
                if (err) throw err;
                db = dbConn;
                done();
            });
        });
        it('and', function () {
            var queryString = db.collection(mochaTestSheet).filterQuery({a:'b', c:'d'});
            assert.equal(queryString, ' a = "b" and c = "d"');
        });
        it('$or', function () {
            var queryString = db.collection(mochaTestSheet).filterQuery({$or:[{a:'b'},{a:'c'}]});
            assert.equal(queryString, '  (  a = "b" or a = "c" ) ');
        });
        it('$gt', function() {
            var queryString = db.collection(mochaTestSheet).filterQuery({a:{$gt:1}});
            assert.equal(queryString, ' a > 1');
        });
        it('$lt', function() {
            var queryString = db.collection(mochaTestSheet).filterQuery({a:{$lt:1}});
            assert.equal(queryString, ' a < 1');
        });
        it('$gte', function() {
            var queryString = db.collection(mochaTestSheet).filterQuery({a:{$gte:1}});
            assert.equal(queryString, ' a >= 1');
        });
        it('$lte', function() {
            var queryString = db.collection(mochaTestSheet).filterQuery({a:{$lte:1}});
            assert.equal(queryString, ' a <= 1');
        });
        it('$ne', function() {
            var queryString = db.collection(mochaTestSheet).filterQuery({a:{$ne:1}});
            assert.equal(queryString, ' a <> 1');
        });
        it('$in');
        it('$nin');
        it('$exists');
        it('Projections http://docs.mongodb.org/master/tutorial/project-fields-from-query-results/');
        it('Modifiers http://docs.mongodb.org/manual/reference/operator/query-modifier/');
        it('composite query', function() {
            var queryString = db.collection(mochaTestSheet).filterQuery( {b:4, $or:[{a:{$gt:1}}, {a:3}]} );
            assert.equal(queryString, ' b = 4 and  (  a > 1 or a = 3 ) ');
        });
    });
    describe('#Collection.projectionFields()', function () {
        before(function (done) {
            this.timeout(30000);
            geepers.connect(geepersId, function (err, dbConn) {
                if (err) throw err;
                db = dbConn;
                done();
            });
        });
        it('handles empty projection, returns all fields', function () {
            var fields = db.collection(mochaTestSheet).projectionFields({});
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.gid, 1, 'No gid');
            assert.equal(fields.i, 1, 'No i');
            assert.equal(fields.time, 1, 'No time');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fieldKeys.length, 4, 'Incorrect field count');
        });
        it('handles null projection, returns all fields', function () {
            var fields = db.collection(mochaTestSheet).projectionFields(null);
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.gid, 1, 'No gid');
            assert.equal(fields.i, 1, 'No i');
            assert.equal(fields.time, 1, 'No time');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fieldKeys.length, 4, 'Incorrect field count');
        });
        it('handles projection exclude gid, returns all other fields', function () {
            var fields = db.collection(mochaTestSheet).projectionFields({gid:0});
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.i, 1, 'No i');
            assert.equal(fields.time, 1, 'No time');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fieldKeys.length, 3, 'Incorrect field count');
        });
        it('can exclude specific fields, returns all other fields', function () {
            var fields = db.collection(mochaTestSheet).projectionFields({time:0});
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.i, 1, 'No i');
            assert.equal(fields.gid, 1, 'No gid');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fieldKeys.length, 3, 'Incorrect field count');
        });
        it('can exclude specific field and gid, returns all other fields', function () {
            var fields = db.collection(mochaTestSheet).projectionFields({time:0, gid:0});
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.i, 1, 'No i');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fieldKeys.length, 2, 'Incorrect field count');
        });
        it('can include specific fields, returns only those fields and gid', function () {
            var fields = db.collection(mochaTestSheet).projectionFields({time:1, word:1});
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.time, 1, 'No time');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fields.gid, 1, 'No gid');
            assert.equal(fieldKeys.length, 3, 'Incorrect field count');
        });
        it('can include specific fields exclude gid, returns only those fields', function () {
            var fields = db.collection(mochaTestSheet).projectionFields({time:1, word:1, gid:0});
            var fieldKeys = Object.keys(fields);
            assert.equal(fields.time, 1, 'No time');
            assert.equal(fields.word, 1, 'No word');
            assert.equal(fieldKeys.length, 2, 'Incorrect field count');
        });
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
            db.collection(mochaTestSheet).find({},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, testData.length);
                done(err);
            });
        });
        it('equals 22 filter, returns only 1 record, with ', function (done) {
            db.collection(mochaTestSheet).find({i:22},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 1);
                assert.equal(result[0].i, 22);
                done(err);
            });
        });
        it('Greater than 22 filter returns only those greater, 7 records', function (done) { 
            db.collection(mochaTestSheet).find({i:{$gt:22}},{},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 7);
                done(err);
            });
        });
        it('Less than 22 filter returns only those less than, 2 records', function (done) { 
            db.collection(mochaTestSheet).find({i:{$lt:22}},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 2);
                done(err);
            });
        });
        it('Greater or equal than 22 filter returns only those greater or equal, 8 records', function (done) { 
            db.collection(mochaTestSheet).find({i:{$gte:22}},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 8);
                done(err);
            });
        });
        it('Less or equal than 22 filter returns only those less than, 3 records', function (done) { 
            db.collection(mochaTestSheet).find({i:{$lte:22}},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 3);
                done(err);
            });
        });
        it('or filter 22 or 23 return only those 2 records', function (done) { 
            db.collection(mochaTestSheet).find({$or:[{i:22},{i:23}]},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 2);
                assert.equal((result[0].i == 22 || result[1].i == 22), true);
                assert.equal((result[0].i == 23 || result[1].i == 23), true);
                done(err);
            });
        });
        it('equals filter "i = 28", returns 2 records, correct time values ', function (done) {
            db.collection(mochaTestSheet).find({i:28},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 2);
                assert.equal(result[0].i, 28);
                assert.equal(result[0].time, 282828);
                assert.equal(result[1].i, 28);
                assert.equal(result[1].time, 292929);
                done(err);
            });
        });
        it('and filter "i == 28 and time == 282828" returns 1 records', function (done) {
            db.collection(mochaTestSheet).find({i:28,time:282828},function (err, result) {
                result = result.toArray();
                assert.equal(result.length, 1);
                assert.equal(result[0].time, 282828);
                assert.equal(result[0].i, 28);
                done(err);
            });
        });
        it('and filter on string property "word == "this is 20" returns 1 records', function (done) {
            db.collection(mochaTestSheet).find({word:'this is 20'},function (err, result) {
                result = result.toArray();
                assert.equal(err, null);
                assert.notEqual(result, null);
                assert.equal(result.length, 1);
                assert.equal(result[0].time, 202020);
                assert.equal(result[0].i, 20);
                done(err);
            });
        });
        it('not equal 28 returns 8 records', function (done) {
            db.collection(mochaTestSheet).find({i:{$ne:28}},function (err, result) {
                result = result.toArray();
                assert.equal(err, null);
                assert.notEqual(result, null);
                assert.equal(result.length, 8);
                done(err);
            });
        });
        it('equal 22 returns 1 record, projection for time only plus gid, returns correct', function (done) {
            db.collection(mochaTestSheet).find({i:22},{time:1},function (err, result) {
                result = result.toArray();
                assert.equal(err, null);
                assert.notEqual(result, null);
                assert.equal(result.length, 1);
                assert.equal(result[0].time, 222222)
                assert.equal(result[0].i, null);
                assert.notEqual(result[0].gid, null);
                done(err);
            });
        });
        it('equal 22 returns 1 record, projection to exclude time only, returns correct', function (done) {
            db.collection(mochaTestSheet).find({i:22},{time:0},function (err, result) {
                result = result.toArray();
                assert.equal(err, null);
                assert.notEqual(result, null);
                assert.equal(result.length, 1);
                assert.equal(result[0].time, null)
                assert.equal(result[0].i, 22);
                assert.equal(result[0].word, 'this is 22');
                assert.notEqual(result[0].gid, null);
                done(err);
            });
        });
        it('sort order');
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
               assert.equal(result.length, 1);
               assert.equal(result[0].i, 1);
               assert.equal(result[0].time, null);
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
                   result = result.toArray();
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
                    result = result.toArray();
                    assert.equal(testData.length - 1, results.length);
                    db.collection(mochaTestSheet).find({i:20},{},function (err, results) {
                        result = result.toArray();
                        assert.equal(0, results.length);
                        done();
                    });
                });
            });
        });
        it('delete by filter > 26 reduces count by 3, and removes expected', function (done) {
            db.collection(mochaTestSheet).deleteMany({i: {$gt: 26}},{},function (err, result) {
                db.collection(mochaTestSheet).find({},{},function (err, results) {
                    result = result.toArray();
                    assert.equal(testData.length - 3, results.length);
                    db.collection(mochaTestSheet).find({i: {$gt: 26}},{},function (err, results) {
                        result = result.toArray();
                        assert.equal(0, results.length);
                        done();
                    });
                });
            });
        });
    });
    describe('#Cursor.sort()', function () {
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
        it.only('finds all in reverse i order', function (done) { 
            db.collection(mochaTestSheet).find({},function (err, result) {
                result = result.sort({i:-1}).toArray();
                console.log(result);
                assert.equal(result.length, 10);
                done(err);
            });
        });
    });
});

