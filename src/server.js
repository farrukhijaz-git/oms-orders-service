'use strict';

require('dotenv').config();

const app = require('./app');

const PORT = process.env.PORT || 3002;

app.listen(PORT, () => {
  console.log(`oms-orders-service listening on port ${PORT}`);
});
