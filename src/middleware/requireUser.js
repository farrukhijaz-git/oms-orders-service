'use strict';

module.exports = (req, res, next) => {
  const userId = req.headers['x-user-id'];
  if (!userId) {
    return res.status(401).json({ error: { code: 'UNAUTHORIZED', message: 'Authentication required' } });
  }
  req.userId = userId;
  req.userRole = req.headers['x-user-role'];
  next();
};
