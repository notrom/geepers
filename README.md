# geepers

A simple Node.js library for accessing Google Spreadsheets. It provides an API similar
to that of MongoDB for CRUD operations making it familiar to many developers.

Uses the [node-google-spreadsheet](https://github.com/theoephraim/node-google-spreadsheet) 
module for accessing Goole Spreadsheets, and provides thin Mongo like wrapper around 
it's operations.

If you're looking for a speedy database, this module is **not** it. Operations that alter 
the data are *slooooowwwww*. Operations data read operations are faster but don't expect 
to break any records.

## Installation

```
npm install geepers
```

## Getting Started

### Create a Spreadsheet
Manually create a spreadsheet that you want to access with geepers. Within that spreadsheet,
you can optionally create new sheet with a meaningful name, or feel free to stick with "Sheet1".
Make a note of the id of the spreadsheet, this is found in the URL. For example, the spreadsheet 
id in the URL `https://docs.google.com/spreadsheets/d/abc1234567/edit#gid=0` is "abc1234567".
You'll need that id shortly.

### Google Spreadsheet Authentication
geepers only supports authenticated access to Google Spreadsheets using service accounts.
Setting a spreadsheet up for this is a few steps described in the 
[node-google-spreadsheet docs](https://github.com/theoephraim/node-google-spreadsheet#service-account-recommended-method).

### geepers Configuration
Once you've followed the steps to enable API access via a service account, you'll need to 
update the provided JSON key file, adding the id of your shared spreadsheet.

Open the JSON key file and add a new key `geepers_sheet_id` with the value set to the
id of the spreadsheet you noted earlier. It should end up looking something like below.

```json
{
  "geepers_sheet_id": "your google sheet id",
  "private_key_id": "key id ...",
  "private_key": "key ...",
  "client_email": "your_service_account@developer.gserviceaccount.com",
  "client_id": "your_service_account.apps.googleusercontent.com",
  "type": "service_account"
}
```

## Example Usage

```javascript
var Geepers = require('geepers.js');
var geepersConfig = require('./gsheetsauth.json');

var geepers = new Geepers();

geepers.connect(geepersConfig, function (err, db) {
    // Collection name maps to spreadsheet sheet name
    db.collection('Sheet1').insertMany([
        { name: 'Superman', alignment: 'Hero', comic: 'DC' },
        { name: 'Batman', alignment: 'Hero', comic: 'DC' },
        { name: 'Joker', alignment: 'Villian', comic: 'DC' },
        { name: 'Iron Man', alignment: 'Hero', comic: 'Marvel' },
        { name: 'Thor', alignment: 'Hero', comic: 'Marvel' },
        { name: 'Jean Grey', alignment: 'Hero', comic: 'Marvel' },
        { name: 'Ultron', alignment: 'Villian', comic: 'Marvel' }],
        function (err, insertedRecs) {
            // find all the DC heros
            db.collection('Sheet1').find(
                {alignment: 'Hero', comic: 'DC'},
                {gid:1, name:1},
                function (err, dcHeros) {
                    var hero = {};
                    // delete all the DC heros
                    while (dcHeros.hasNext()) {
                        hero = dcHeros.next();
                        db.collection('Sheet1').deleteMany({gid:hero.gid},
                            function(err, result) {
                                if (err || !result) {
                                    console.log('Delete error for - ', hero);
                                }
                            }
                        );
                    }
                }
            );
            // Update a record
            db.collection('Sheet1').find(
                { name: 'Jean Grey' },
                function (err, jeanGreys) {
                    if (jeanGreys.count() === 1) {
                        var jean = jeanGreys.toArray()[0];
                        // Jean Grey goes bad
                        db.collection('Sheet1').update(
                            {gid:jean.gid},
                            {name: 'Dark Phoenix', alignment: 'Villian'},
                            function (err) {
                                if (err) {
                                    console.log('Error while updating - ', jean);
                                }
                            }
                        )
                    }
                }
            );
        });
});
```

Take a look at tests/test.js to see more examples.

## Query Options

Query selectors can use a subset of MongoDB query operators. The default action is
to and the listed selection properties. Aside from and, the following operators 
are supported.
- `$or` - or
- `$gt` and `$gte` - greater than and greater than or equal to 
- `$lt` and `$lte` - less than and less than or equal to
- `$ne` - not equal to

You can test a query against a specific collection (a sheet) using the collection's
`filterQuery()` function. This returns a string representation of the query.

## Find Projections

Projections specify which properties are going to be returned in the found records.

A projection object is either inclusive or exclusive, except for the `gid` property 
which can be excluded from an inclusive projection.

The `gid` property is always returned unless explicitly excluded

A projection object can be supplied as the second arguement to the `find()` function, it's
an optional arguement, if it's not provided all fields are returned.

### Examples
- `{a:1, b:1}` - returns properties `a`, `b`, and `gid`
- `{gid:-1, a:1, b:1}` - returns only `a` and `b`
- `{a:-1}` - returns all properties except `a` 

# Running Tests

Establish a spreadsheet and set it up for geepers access. Tests require the JSON key file
be named `gsheetsauth.json` and located in the root directory of the project.

Create a new empty sheet called `functionTests`.

```
npm test
```

## Links

- <https://developers.google.com/google-apps/spreadsheets/>
- <https://github.com/theoephraim/node-google-spreadsheet/>

## Todo
- Better documentation of geepers API
- Regex string matches in queries
- Spreadsheet maintenance, create, sheets
- Speed

