// Fires at 00:01 UTC on the 1st of each month.
// Just invokes starling-sync — the boundary-crossing logic inside that function
// detects the UK month rollover and writes the balance to the previous month's cell.
const { handler } = require('./starling-sync.js');

exports.handler = handler;
exports.config = { schedule: '1 0 1 * *' };
