# geepers

A simple Node.js library for accessing Google Spreadsheets. It provides an API similar
to that of MongoDB for CRUD operations making it familiar to many developers.

Uses the [node-google-spreadsheet](https://github.com/theoephraim/node-google-spreadsheet) 
module for accessing Goole Spreadsheets, and provides thin Mongo like wrapper around 
it's operations.

## Installation

```
npm install geepers
```

```javascript
var geepers = require('geepers');
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
var geepers = require('geepers');




```
