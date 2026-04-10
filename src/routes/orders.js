'use strict';

const express = require('express');
const multer = require('multer');
const { parse } = require('csv-parse');

const pool = require('../db');
const requireUser = require('../middleware/requireUser');

const router = express.Router();

// All routes require an authenticated user
router.use(requireUser);

// Multer: memory storage for CSV uploads
const upload = multer({ storage: multer.memoryStorage() });

// Valid enum values
const VALID_STATUSES = ['new', 'label_generated', 'inventory_ordered', 'packed', 'ready', 'shipped', 'delivered', 'cancelled'];
const VALID_PLATFORMS = ['walmart', 'ebay', 'amazon', 'manual'];

// ---------------------------------------------------------------------------
// GET /orders/dashboard
// MUST be defined before /orders/:id to avoid being swallowed by the param route
// ---------------------------------------------------------------------------
router.get('/dashboard', async (req, res, next) => {
  try {
    // Status counts
    const countsResult = await pool.query(
      `SELECT status, COUNT(*)::int AS count FROM orders.orders GROUP BY status`
    );

    const counts = {
      new: 0,
      label_generated: 0,
      inventory_ordered: 0,
      packed: 0,
      ready: 0,
      shipped: 0,
      delivered: 0,
    };
    for (const row of countsResult.rows) {
      counts[row.status] = row.count;
    }

    // Recent activity: last 10 status-log entries
    const activityResult = await pool.query(
      `SELECT
         sl.id,
         sl.order_id,
         o.external_id,
         o.customer_name,
         sl.from_status,
         sl.to_status,
         sl.note,
         sl.changed_at,
         u.display_name AS changed_by_name
       FROM orders.order_status_log sl
       JOIN orders.orders o ON o.id = sl.order_id
       LEFT JOIN app.users u ON u.id = sl.changed_by
       ORDER BY sl.changed_at DESC
       LIMIT 10`
    );

    // At-risk orders: ship_by or deliver_by within 7 days and not yet at the required status
    const atRiskResult = await pool.query(
      `SELECT
         id, external_id, customer_name, status, platform,
         ship_by_date, deliver_by_date
       FROM orders.orders
       WHERE status NOT IN ('cancelled')
         AND (
           (ship_by_date IS NOT NULL
            AND ship_by_date <= NOW() + INTERVAL '7 days'
            AND status NOT IN ('shipped', 'delivered'))
           OR
           (deliver_by_date IS NOT NULL
            AND deliver_by_date <= NOW() + INTERVAL '7 days'
            AND status NOT IN ('delivered'))
         )
       ORDER BY
         LEAST(
           CASE WHEN status NOT IN ('shipped','delivered') THEN ship_by_date ELSE NULL END,
           CASE WHEN status NOT IN ('delivered') THEN deliver_by_date ELSE NULL END
         ) ASC NULLS LAST
       LIMIT 20`
    );

    res.json({ counts, recent_activity: activityResult.rows, at_risk_orders: atRiskResult.rows });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /orders/recently-viewed
// ---------------------------------------------------------------------------
router.get('/recently-viewed', async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT
         rv.viewed_at,
         o.id,
         o.external_id,
         o.platform,
         o.customer_name,
         o.city,
         o.state,
         o.status,
         o.label_id,
         o.tracking_number,
         o.created_at,
         o.updated_at
       FROM orders.recently_viewed rv
       JOIN orders.orders o ON o.id = rv.order_id
       WHERE rv.user_id = $1
       ORDER BY rv.viewed_at DESC
       LIMIT 10`,
      [req.userId]
    );

    res.json({ orders: result.rows });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /orders/import/csv
// ---------------------------------------------------------------------------
router.post('/import/csv', upload.single('file'), async (req, res, next) => {
  if (!req.file) {
    return res.status(400).json({ error: { code: 'BAD_REQUEST', message: 'No file uploaded. Use field name "file".' } });
  }

  let rows;
  try {
    rows = await new Promise((resolve, reject) => {
      parse(req.file.buffer, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      }, (err, records) => {
        if (err) reject(err);
        else resolve(records);
      });
    });
  } catch (err) {
    return res.status(400).json({ error: { code: 'BAD_REQUEST', message: `CSV parse error: ${err.message}` } });
  }

  // Group rows by Purchase Order #
  const groups = {};
  for (const row of rows) {
    const po = row['Purchase Order #'];
    if (!po) continue;
    if (!groups[po]) groups[po] = [];
    groups[po].push(row);
  }

  let imported = 0;
  let skipped = 0;
  const errors = [];

  for (const [externalId, orderRows] of Object.entries(groups)) {
    // Check if already exists
    try {
      const existing = await pool.query(
        `SELECT id FROM orders.orders WHERE external_id = $1 LIMIT 1`,
        [externalId]
      );
      if (existing.rows.length > 0) {
        skipped++;
        continue;
      }

      const firstRow = orderRows[0];
      const customerName = firstRow['Customer Name'] || '';
      const addressLine1 = firstRow['Ship To Address1'] || '';
      const addressLine2 = firstRow['Ship To Address2'] || '';
      const city = firstRow['Ship To City'] || '';
      const state = firstRow['Ship To State'] || '';
      const zip = String(firstRow['Ship To Zip'] || '');

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        const orderInsert = await client.query(
          `INSERT INTO orders.orders
             (external_id, platform, customer_name, address_line1, address_line2,
              city, state, zip, country, status, created_by)
           VALUES ($1, 'walmart', $2, $3, $4, $5, $6, $7, 'US', 'new', $8)
           RETURNING id`,
          [externalId, customerName, addressLine1, addressLine2, city, state, zip, req.userId]
        );
        const orderId = orderInsert.rows[0].id;

        for (const row of orderRows) {
          const sku = row['Item Id'] || null;
          const name = row['Item Description'] || '';
          const quantity = parseInt(row['Qty'], 10) || 1;
          const unitPrice = parseFloat(row['Unit Price']) || 0;

          await client.query(
            `INSERT INTO orders.order_items (order_id, sku, name, quantity, unit_price)
             VALUES ($1, $2, $3, $4, $5)`,
            [orderId, sku, name, quantity, unitPrice]
          );
        }

        await client.query(
          `INSERT INTO orders.order_status_log (order_id, from_status, to_status, changed_by, note)
           VALUES ($1, NULL, 'new', $2, 'Imported via CSV')`,
          [orderId, req.userId]
        );

        await client.query('COMMIT');
        imported++;
      } catch (err) {
        await client.query('ROLLBACK');
        errors.push({ external_id: externalId, message: err.message });
      } finally {
        client.release();
      }
    } catch (err) {
      errors.push({ external_id: externalId, message: err.message });
    }
  }

  res.json({ imported, skipped, errors });
});

// ---------------------------------------------------------------------------
// GET /orders/by-external-id/:externalId
// Used internally by other services (e.g. Walmart poller) to look up an order
// by its marketplace ID without requiring direct DB access to orders schema.
// ---------------------------------------------------------------------------
router.get('/by-external-id/:externalId', async (req, res, next) => {
  try {
    const { externalId } = req.params;
    const result = await pool.query(
      `SELECT id, status, tracking_number, order_date, ship_by_date,
              deliver_by_date, ship_node, order_total, ship_node_id,
              walmart_status, total_tax, customer_order_id, customer_email,
              phone, address_type, shipping_method
       FROM orders.orders WHERE external_id = $1 LIMIT 1`,
      [externalId]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ order: null });
    }
    res.json({ order: result.rows[0] });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /orders - List with filtering and pagination
// ---------------------------------------------------------------------------
router.get('/', async (req, res, next) => {
  try {
    const {
      status,
      walmart_status,
      platform,
      search,
      date_from,
      date_to,
      ship_node,
      ship_by_from,
      ship_by_to,
      deliver_by_from,
      deliver_by_to,
      sort_by,
      sort_dir,
      page = 1,
      limit = 20,
    } = req.query;

    const pageNum = Math.max(1, parseInt(page, 10) || 1);
    const limitNum = Math.min(100, Math.max(1, parseInt(limit, 10) || 20));
    const offset = (pageNum - 1) * limitNum;

    const conditions = [];
    const params = [];

    if (status) {
      const statuses = status.split(',').map((s) => s.trim()).filter(Boolean);
      if (statuses.length > 0) {
        params.push(statuses);
        conditions.push(`o.status = ANY($${params.length})`);
      }
    }

    if (walmart_status) {
      const wStatuses = walmart_status.split(',').map((s) => s.trim()).filter(Boolean);
      if (wStatuses.length > 0) {
        params.push(wStatuses);
        conditions.push(`o.walmart_status = ANY($${params.length})`);
      }
    }

    if (platform) {
      params.push(platform.trim());
      conditions.push(`o.platform = $${params.length}`);
    }

    if (search) {
      params.push(`%${search.trim()}%`);
      conditions.push(`(o.customer_name ILIKE $${params.length} OR o.external_id ILIKE $${params.length})`);
    }

    if (date_from) {
      params.push(date_from);
      conditions.push(`COALESCE(o.order_date, o.created_at) >= $${params.length}`);
    }

    if (date_to) {
      params.push(date_to);
      conditions.push(`COALESCE(o.order_date, o.created_at) <= $${params.length}`);
    }

    if (ship_node) {
      params.push(`%${ship_node.trim()}%`);
      conditions.push(`o.ship_node ILIKE $${params.length}`);
    }

    if (ship_by_from) {
      params.push(ship_by_from);
      conditions.push(`o.ship_by_date >= $${params.length}`);
    }

    if (ship_by_to) {
      params.push(ship_by_to);
      conditions.push(`o.ship_by_date <= $${params.length}`);
    }

    if (deliver_by_from) {
      params.push(deliver_by_from);
      conditions.push(`o.deliver_by_date >= $${params.length}`);
    }

    if (deliver_by_to) {
      params.push(deliver_by_to);
      conditions.push(`o.deliver_by_date <= $${params.length}`);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const SORT_COLUMNS = {
      order_date:     `COALESCE(o.order_date, o.created_at)`,
      ship_by_date:   `o.ship_by_date`,
      deliver_by_date:`o.deliver_by_date`,
      order_total:    `o.order_total`,
      customer_name:  `o.customer_name`,
    };
    const sortCol = SORT_COLUMNS[sort_by] || `COALESCE(o.order_date, o.created_at)`;
    const sortDir = sort_dir === 'asc' ? 'ASC' : 'DESC';
    const orderBy = `${sortCol} ${sortDir} NULLS LAST`;

    // Count query
    const countResult = await pool.query(
      `SELECT COUNT(*)::int AS total FROM orders.orders o ${where}`,
      params
    );
    const total = countResult.rows[0].total;

    // Data query
    params.push(limitNum);
    const limitParam = params.length;
    params.push(offset);
    const offsetParam = params.length;

    const dataResult = await pool.query(
      `SELECT
         o.id,
         o.external_id,
         o.platform,
         o.customer_name,
         o.address_line1,
         o.address_line2,
         o.city,
         o.state,
         o.zip,
         o.country,
         o.status,
         o.walmart_status,
         o.label_id,
         o.tracking_number,
         o.order_date,
         o.ship_by_date,
         o.deliver_by_date,
         o.ship_node,
         o.ship_node_id,
         o.order_total,
         o.total_tax,
         o.customer_order_id,
         o.customer_email,
         o.tracking_pushed_to_walmart,
         o.created_at,
         o.updated_at,
         COUNT(oi.id)::int AS item_count
       FROM orders.orders o
       LEFT JOIN orders.order_items oi ON oi.order_id = o.id
       ${where}
       GROUP BY o.id
       ORDER BY ${orderBy}
       LIMIT $${limitParam} OFFSET $${offsetParam}`,
      params
    );

    res.json({
      orders: dataResult.rows,
      total,
      page: pageNum,
      limit: limitNum,
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /orders - Create order manually
// ---------------------------------------------------------------------------
router.post('/', async (req, res, next) => {
  try {
    const {
      external_id,
      customer_name,
      address_line1,
      address_line2,
      city,
      state,
      zip,
      country,
      platform,
      items,
      notes,
      status: providedStatus,
      tracking_number,
      order_date,
      ship_by_date,
      deliver_by_date,
      ship_node,
      ship_node_id,
      order_total,
      total_tax,
      walmart_status,
      customer_order_id,
      customer_email,
      phone,
      address_type,
      shipping_method,
      carrier_method,
      ship_method,
    } = req.body;

    // Allow callers (e.g. Walmart poller) to set initial status; default to 'new'
    const initialStatus = (providedStatus && VALID_STATUSES.includes(providedStatus))
      ? providedStatus
      : 'new';

    // Validate required fields
    const missing = [];
    if (!customer_name) missing.push('customer_name');
    if (!address_line1) missing.push('address_line1');
    if (!city) missing.push('city');
    if (!state) missing.push('state');
    if (!zip) missing.push('zip');
    if (!items || !Array.isArray(items) || items.length === 0) missing.push('items');

    if (missing.length > 0) {
      return res.status(400).json({
        error: {
          code: 'VALIDATION_ERROR',
          message: `Missing required fields: ${missing.join(', ')}`,
        },
      });
    }

    // Validate items
    for (let i = 0; i < items.length; i++) {
      if (!items[i].name) {
        return res.status(400).json({
          error: {
            code: 'VALIDATION_ERROR',
            message: `Item at index ${i} is missing required field: name`,
          },
        });
      }
    }

    // Validate platform if provided
    if (platform && !VALID_PLATFORMS.includes(platform)) {
      return res.status(400).json({
        error: {
          code: 'VALIDATION_ERROR',
          message: `Invalid platform. Must be one of: ${VALID_PLATFORMS.join(', ')}`,
        },
      });
    }

    const client = await pool.connect();
    let createdOrder;
    try {
      await client.query('BEGIN');

      const orderResult = await client.query(
        `INSERT INTO orders.orders
           (external_id, platform, customer_name, address_line1, address_line2,
            city, state, zip, country, status, tracking_number, notes, created_by,
            order_date, ship_by_date, deliver_by_date, ship_node, ship_node_id,
            order_total, total_tax, walmart_status, customer_order_id, customer_email,
            phone, address_type, shipping_method, carrier_method, ship_method)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                 $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
         ON CONFLICT (platform, external_id) DO NOTHING
         RETURNING *`,
        [
          external_id || null,
          platform || 'manual',
          customer_name,
          address_line1,
          address_line2 || null,
          city,
          state,
          zip,
          country || null,
          initialStatus,
          tracking_number || null,
          notes || null,
          req.userId,
          order_date || null,
          ship_by_date || null,
          deliver_by_date || null,
          ship_node || null,
          ship_node_id || null,
          order_total || null,
          total_tax || null,
          walmart_status || null,
          customer_order_id || null,
          customer_email || null,
          phone || null,
          address_type || null,
          shipping_method || null,
          carrier_method || null,
          ship_method || null,
        ]
      );
      // ON CONFLICT DO NOTHING returns zero rows if external_id already exists
      if (orderResult.rows.length === 0) {
        await client.query('ROLLBACK');
        client.release();
        return res.status(409).json({
          error: { code: 'DUPLICATE', message: `Order with external_id '${external_id}' on platform '${platform}' already exists` },
        });
      }
      createdOrder = orderResult.rows[0];

      for (const item of items) {
        await client.query(
          `INSERT INTO orders.order_items
             (order_id, sku, name, quantity, unit_price, line_number, condition,
              tax_amount, line_tracking_number, tracking_url, ship_datetime, line_status)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
          [
            createdOrder.id,
            item.sku || null,
            item.name,
            item.quantity || 1,
            item.unit_price || 0,
            item.line_number || null,
            item.condition || null,
            item.tax_amount || null,
            item.line_tracking_number || null,
            item.tracking_url || null,
            item.ship_datetime || null,
            item.line_status || null,
          ]
        );
      }

      await client.query(
        `INSERT INTO orders.order_status_log (order_id, from_status, to_status, changed_by, note)
         VALUES ($1, NULL, $2, $3, $4)`,
        [
          createdOrder.id,
          initialStatus,
          req.userId,
          initialStatus === 'new' ? 'Order created' : `Order created (imported as ${initialStatus})`,
        ]
      );

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    // Fetch created order with items
    const itemsResult = await pool.query(
      `SELECT * FROM orders.order_items WHERE order_id = $1`,
      [createdOrder.id]
    );

    res.status(201).json({ order: { ...createdOrder, items: itemsResult.rows } });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /orders/:id - Full order with items and status log
// ---------------------------------------------------------------------------
router.get('/:id', async (req, res, next) => {
  try {
    const { id } = req.params;

    const orderResult = await pool.query(
      `SELECT * FROM orders.orders WHERE id = $1`,
      [id]
    );

    if (orderResult.rows.length === 0) {
      return res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Order not found' } });
    }

    const order = orderResult.rows[0];

    const itemsResult = await pool.query(
      `SELECT * FROM orders.order_items WHERE order_id = $1 ORDER BY id`,
      [id]
    );

    const logResult = await pool.query(
      `SELECT
         sl.id,
         sl.from_status,
         sl.to_status,
         sl.note,
         sl.changed_at,
         sl.changed_by,
         u.display_name AS changed_by_name
       FROM orders.order_status_log sl
       LEFT JOIN app.users u ON u.id = sl.changed_by
       WHERE sl.order_id = $1
       ORDER BY sl.changed_at ASC`,
      [id]
    );

    res.json({
      order: {
        ...order,
        items: itemsResult.rows,
        status_log: logResult.rows,
      },
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PATCH /orders/:id/status - Update order status
// ---------------------------------------------------------------------------
router.patch('/:id/status', async (req, res, next) => {
  try {
    const { id } = req.params;
    const { status, note, tracking_number } = req.body;

    if (!status) {
      return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'status is required' } });
    }

    if (!VALID_STATUSES.includes(status)) {
      return res.status(400).json({
        error: {
          code: 'VALIDATION_ERROR',
          message: `Invalid status. Must be one of: ${VALID_STATUSES.join(', ')}`,
        },
      });
    }

    // Get current status
    const currentResult = await pool.query(
      `SELECT id, status FROM orders.orders WHERE id = $1`,
      [id]
    );

    if (currentResult.rows.length === 0) {
      return res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Order not found' } });
    }

    const fromStatus = currentResult.rows[0].status;

    const client = await pool.connect();
    let updatedOrder;
    try {
      await client.query('BEGIN');

      // Build update query dynamically based on whether tracking_number is provided
      let updateQuery;
      let updateParams;

      if (tracking_number !== undefined) {
        updateQuery = `
          UPDATE orders.orders
          SET status = $1, tracking_number = $2, updated_at = now()
          WHERE id = $3
          RETURNING *`;
        updateParams = [status, tracking_number, id];
      } else {
        updateQuery = `
          UPDATE orders.orders
          SET status = $1, updated_at = now()
          WHERE id = $2
          RETURNING *`;
        updateParams = [status, id];
      }

      const updateResult = await client.query(updateQuery, updateParams);
      updatedOrder = updateResult.rows[0];

      await client.query(
        `INSERT INTO orders.order_status_log (order_id, from_status, to_status, changed_by, note)
         VALUES ($1, $2, $3, $4, $5)`,
        [id, fromStatus, status, req.userId, note || null]
      );

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    res.json({ order: updatedOrder });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PATCH /orders/:id - Update order label, tracking, and metadata fields
// ---------------------------------------------------------------------------
router.patch('/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    const {
      label_id, tracking_number, order_date, ship_by_date, deliver_by_date,
      ship_node, ship_node_id, order_total, total_tax, walmart_status,
      customer_order_id, customer_email, phone, address_type,
      shipping_method, carrier_method, ship_method, tracking_pushed_to_walmart,
    } = req.body;

    // At least one field must be provided
    if (
      label_id === undefined &&
      tracking_number === undefined &&
      order_date === undefined &&
      ship_by_date === undefined &&
      deliver_by_date === undefined &&
      ship_node === undefined &&
      ship_node_id === undefined &&
      order_total === undefined &&
      total_tax === undefined &&
      walmart_status === undefined &&
      customer_order_id === undefined &&
      customer_email === undefined &&
      phone === undefined &&
      address_type === undefined &&
      shipping_method === undefined &&
      carrier_method === undefined &&
      ship_method === undefined &&
      tracking_pushed_to_walmart === undefined
    ) {
      return res.status(400).json({
        error: {
          code: 'VALIDATION_ERROR',
          message: 'At least one field must be provided',
        },
      });
    }

    // Build dynamic update query
    const updates = [];
    const params = [];
    let paramIndex = 1;

    if (label_id !== undefined) {
      updates.push(`label_id = $${paramIndex}`);
      params.push(label_id);
      paramIndex++;
    }

    if (tracking_number !== undefined) {
      updates.push(`tracking_number = $${paramIndex}`);
      params.push(tracking_number);
      paramIndex++;
    }

    if (order_date !== undefined) {
      updates.push(`order_date = $${paramIndex}`);
      params.push(order_date);
      paramIndex++;
    }

    if (ship_by_date !== undefined) {
      updates.push(`ship_by_date = $${paramIndex}`);
      params.push(ship_by_date);
      paramIndex++;
    }

    if (deliver_by_date !== undefined) {
      updates.push(`deliver_by_date = $${paramIndex}`);
      params.push(deliver_by_date);
      paramIndex++;
    }

    if (ship_node !== undefined) {
      updates.push(`ship_node = $${paramIndex}`);
      params.push(ship_node);
      paramIndex++;
    }

    if (order_total !== undefined) {
      updates.push(`order_total = $${paramIndex}`);
      params.push(order_total);
      paramIndex++;
    }

    if (ship_node_id !== undefined) {
      updates.push(`ship_node_id = $${paramIndex}`);
      params.push(ship_node_id);
      paramIndex++;
    }

    if (total_tax !== undefined) {
      updates.push(`total_tax = $${paramIndex}`);
      params.push(total_tax);
      paramIndex++;
    }

    if (walmart_status !== undefined) {
      updates.push(`walmart_status = $${paramIndex}`);
      params.push(walmart_status);
      paramIndex++;
    }

    if (customer_order_id !== undefined) {
      updates.push(`customer_order_id = $${paramIndex}`);
      params.push(customer_order_id);
      paramIndex++;
    }

    if (customer_email !== undefined) {
      updates.push(`customer_email = $${paramIndex}`);
      params.push(customer_email);
      paramIndex++;
    }

    if (phone !== undefined) {
      updates.push(`phone = $${paramIndex}`);
      params.push(phone);
      paramIndex++;
    }

    if (address_type !== undefined) {
      updates.push(`address_type = $${paramIndex}`);
      params.push(address_type);
      paramIndex++;
    }

    if (shipping_method !== undefined) {
      updates.push(`shipping_method = $${paramIndex}`);
      params.push(shipping_method);
      paramIndex++;
    }

    if (carrier_method !== undefined) {
      updates.push(`carrier_method = $${paramIndex}`);
      params.push(carrier_method);
      paramIndex++;
    }

    if (ship_method !== undefined) {
      updates.push(`ship_method = $${paramIndex}`);
      params.push(ship_method);
      paramIndex++;
    }

    if (tracking_pushed_to_walmart !== undefined) {
      updates.push(`tracking_pushed_to_walmart = $${paramIndex}`);
      params.push(tracking_pushed_to_walmart);
      paramIndex++;
    }

    updates.push(`updated_at = now()`);
    params.push(id);

    const updateQuery = `
      UPDATE orders.orders
      SET ${updates.join(', ')}
      WHERE id = $${paramIndex}
      RETURNING *`;

    const result = await pool.query(updateQuery, params);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Order not found' } });
    }

    res.json({ order: result.rows[0] });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /orders/:id/view - Record view for recently viewed
// ---------------------------------------------------------------------------
router.post('/:id/view', async (req, res, next) => {
  try {
    const { id } = req.params;
    const userId = req.userId;

    // Verify order exists
    const orderCheck = await pool.query(
      `SELECT id FROM orders.orders WHERE id = $1`,
      [id]
    );
    if (orderCheck.rows.length === 0) {
      return res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Order not found' } });
    }

    // Upsert into recently_viewed
    await pool.query(
      `INSERT INTO orders.recently_viewed (user_id, order_id, viewed_at)
       VALUES ($1, $2, now())
       ON CONFLICT (user_id, order_id) DO UPDATE SET viewed_at = now()`,
      [userId, id]
    );

    // Keep only the last 10 per user — delete oldest beyond 10
    await pool.query(
      `DELETE FROM orders.recently_viewed
       WHERE user_id = $1
         AND order_id NOT IN (
           SELECT order_id
           FROM orders.recently_viewed
           WHERE user_id = $1
           ORDER BY viewed_at DESC
           LIMIT 10
         )`,
      [userId]
    );

    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
