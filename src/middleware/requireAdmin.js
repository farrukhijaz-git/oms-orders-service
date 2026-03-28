'use strict';

module.exports = (req, res, next) => {
  if (req.userRole !== 'admin') {
    return res.status(403).json({ error: { code: 'FORBIDDEN', message: 'Admin access required' } });
  }
  next();
};
