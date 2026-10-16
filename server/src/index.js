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
  limit: 100
}));
app.use(express.json());

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || 'mysql-service',
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || 'rootpassword',
  database: process.env.MYSQL_DATABASE || 'appdb',
  waitForConnections: true,
  connectionLimit: 10
});

async function initDB(retries = 10, delay = 5000) {
  while (retries > 0) {
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
      console.error('Database not ready, retrying in 5 seconds...');
      retries--;
      await new Promise((res) => setTimeout(res, delay));
    }
  }
  process.exit(1);
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
  if (processingOutbox) return;
  processingOutbox = true;
  let connection;
  try {
    connection = await pool.getConnection();
    while (true) {
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
        await connection.rollback();
        throw error;
      }
    }
  } catch (error) {
    console.error('Outbox delivery failed; will retry:', error);
  } finally {
    if (connection) connection.release();
    processingOutbox = false;
  }
}

app.get('/api/items', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM items ORDER BY created_at DESC');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/items', async (req, res) => {
  const { title, description } = req.body;
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
  const { title, description } = req.body;
  const { id } = req.params;
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
  await producer.connect();
  void processOutbox();
  setInterval(() => void processOutbox(), 1000);
  app.listen(PORT, () => {
    console.log('Express server running on port ' + PORT);
  });
}

start().catch((err) => {
  console.error('Server startup failed:', err);
  process.exit(1);
});