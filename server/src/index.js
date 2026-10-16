const express = require('express');
const mysql = require('mysql2/promise');
const { Kafka } = require('kafkajs');
const cors = require('cors');
const { rateLimit } = require('express-rate-limit');

const app = express();
app.use(cors());
app.use('/api', rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: 100,
  standardHeaders: true,
  legacyHeaders: false
}));
app.use(express.json());

let shuttingDown = false;
let server;
let outboxInterval;
let outboxTask;

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || 'mysql-service',
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || 'rootpassword',
  database: process.env.MYSQL_DATABASE || 'appdb',
  waitForConnections: true,
  connectionLimit: 10
});

async function initDB(retries = 10, delay = 5000) {
  while (retries > 0 && !shuttingDown) {
    try {
      await pool.query(
        'CREATE TABLE IF NOT EXISTS items (' +
        'id INT AUTO_INCREMENT PRIMARY KEY, ' +
        'title VARCHAR(255) NOT NULL, ' +
        'description TEXT, ' +
        'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
      );
      await pool.query(
        'CREATE TABLE IF NOT EXISTS outbox (' +
        'id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, ' +
        'event_type VARCHAR(255) NOT NULL, ' +
        'payload JSON NOT NULL)'
      );
      console.log('MySQL connected and table initialized.');
      return;
    } catch (err) {
      console.error('Database not ready, retrying in 5 seconds...', err);
      retries--;
      await new Promise((res) => setTimeout(res, delay));
    }
  }
  if (!shuttingDown) throw new Error('Database initialization retries exhausted');
}

const kafka = new Kafka({
  clientId: 'api-producer',
  brokers: [process.env.KAFKA_BROKER || 'kafka-service:9092']
});
const producer = kafka.producer();

async function withOutboxEvent(eventType, operation) {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const { result, payload } = await operation(connection);
    if (payload !== null) {
      await connection.query(
        'INSERT INTO outbox (event_type, payload) VALUES (?, ?)',
        [eventType, JSON.stringify(payload)]
      );
    }
    await connection.commit();
    return result;
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

let processingOutbox = false;
async function processOutbox() {
  if (processingOutbox || shuttingDown) return;
  processingOutbox = true;
  let connection;
  try {
    connection = await pool.getConnection();
    for (let processed = 0; processed < 100 && !shuttingDown; processed++) {
      await connection.beginTransaction();
      try {
        const [rows] = await connection.query(
          'SELECT id, event_type, payload FROM outbox ORDER BY id LIMIT 1 FOR UPDATE'
        );
        if (rows.length === 0) {
          await connection.commit();
          break;
        }
        const event = rows[0];
        await producer.send({
          topic: 'item-events',
          messages: [
            {
              key: String(event.payload.id || '0'),
              value: JSON.stringify({
                eventType: event.event_type,
                payload: event.payload
              })
            }
          ]
        });
        await connection.query('DELETE FROM outbox WHERE id = ?', [event.id]);
        await connection.commit();
      } catch (error) {
        try {
          await connection.rollback();
        } catch (rollbackError) {
          console.error('Outbox rollback failed:', rollbackError);
        }
        throw error;
      }
    }
  } catch (error) {
    console.error('Outbox delivery failed; will retry:', error);
  } finally {
    try {
      if (connection) connection.release();
    } catch (error) {
      console.error('Outbox connection release failed:', error);
    } finally {
      processingOutbox = false;
    }
  }
}

app.get('/api/items', async (req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT id, title, description, created_at FROM items ORDER BY created_at DESC'
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/items', async (req, res) => {
  const { title: rawTitle, description = null } = req.body || {};
  if (typeof rawTitle !== 'string' || !rawTitle.trim()) {
    return res.status(400).json({ error: 'Title must be a non-empty string' });
  }
  const title = rawTitle.trim();
  try {
    const newItem = await withOutboxEvent('ITEM_CREATED', async (connection) => {
      const [result] = await connection.query(
        'INSERT INTO items (title, description) VALUES (?, ?)',
        [title, description]
      );
      const item = { id: result.insertId, title, description };
      return { result: item, payload: item };
    });
    res.status(201).json(newItem);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/items/:id', async (req, res) => {
  const { title: rawTitle, description = null } = req.body || {};
  const { id } = req.params;
  if (!/^\d+$/.test(id) || !Number.isSafeInteger(Number(id)) || Number(id) <= 0) {
    return res.status(400).json({ error: 'ID must be a positive integer' });
  }
  if (typeof rawTitle !== 'string' || !rawTitle.trim()) {
    return res.status(400).json({ error: 'Title must be a non-empty string' });
  }
  const title = rawTitle.trim();
  try {
    const updated = await withOutboxEvent('ITEM_UPDATED', async (connection) => {
      const [result] = await connection.query(
        'UPDATE items SET title = ?, description = ? WHERE id = ?',
        [title, description, id]
      );
      if (result.affectedRows === 0) return { result: null, payload: null };
      const item = { id: Number(id), title, description };
      return { result: item, payload: item };
    });
    if (!updated) return res.status(404).send('Not Found');
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/items/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const deleted = await withOutboxEvent('ITEM_DELETED', async (connection) => {
      const [result] = await connection.query('DELETE FROM items WHERE id = ?', [id]);
      if (result.affectedRows === 0) return { result: false, payload: null };
      return { result: true, payload: { id: Number(id) } };
    });
    if (!deleted) return res.status(404).send('Not Found');
    res.status(204).send();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 5000;
async function start() {
  await initDB();
  if (shuttingDown) return;
  await producer.connect();
  if (shuttingDown) return;
  const runOutbox = () => {
    if (!processingOutbox) outboxTask = processOutbox();
  };
  runOutbox();
  outboxInterval = setInterval(runOutbox, 1000);
  server = app.listen(PORT, () => {
    console.log('Express server running on port ' + PORT);
  });
}

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  clearInterval(outboxInterval);
  await startup.catch(() => {});
  try {
    if (server) {
      await new Promise((resolve, reject) => {
        server.close((err) => err ? reject(err) : resolve());
      });
    }
  } catch (err) {
    console.error('Server shutdown failed:', err);
    process.exitCode = 1;
  }
  await outboxTask;
  try {
    await producer.disconnect();
  } catch (err) {
    console.error('Kafka shutdown failed:', err);
    process.exitCode = 1;
  }
  try {
    await pool.end();
  } catch (err) {
    console.error('MySQL shutdown failed:', err);
    process.exitCode = 1;
  }
}

process.on('SIGTERM', () => void shutdown());
process.on('SIGINT', () => void shutdown());

const startup = start();
startup.catch((err) => {
  console.error('Server startup failed:', err);
  process.exitCode = 1;
  void shutdown();
});