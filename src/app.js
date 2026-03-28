'use strict';

const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const morgan = require('morgan');

const ordersRouter = require('./routes/orders');

const app = express();

// Security & logging middleware
app.use(helmet());
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());

// Health check (no auth required)
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'oms-orders-service', timestamp: new Date().toISOString() });
});

// Routes
app.use('/orders', ordersRouter);

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Route not found' } });
});

// Global error handler
app.use((err, req, res, next) => { // eslint-disable-line no-unused-vars
  console.error('Unhandled error:', err);
  const status = err.status || err.statusCode || 500;
  res.status(status).json({
    error: {
      code: err.code || 'INTERNAL_ERROR',
      message: err.message || 'An unexpected error occurred',
    },
  });
});

module.exports = app;
