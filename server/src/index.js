const express = require('express');
const mysql = require('mysql2/promise');
const { Kafka } = require('kafkajs');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

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

async function sendKafkaEvent(eventType, payload) {
  try {
    await producer.send({
      topic: 'item-events',
      messages: [
        {
          key: String(payload.id || '0'),
          value: JSON.stringify({ eventType, payload })
        }
      ]
    });
  } catch (error) {
    console.error('Kafka send error:', error);
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
    const [result] = await pool.query(
      'INSERT INTO items (title, description) VALUES (?, ?)',
      [title, description]
    );
    const newItem = { id: result.insertId, title, description };
    await sendKafkaEvent('ITEM_CREATED', newItem);
    res.status(201).json(newItem);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/items/:id', async (req, res) => {
  const { title, description } = req.body;
  const { id } = req.params;
  try {
    const [result] = await pool.query(
      'UPDATE items SET title = ?, description = ? WHERE id = ?',
      [title, description, id]
    );
    if (result.affectedRows === 0) return res.status(404).send('Not Found');
    const updated = { id: Number(id), title, description };
    await sendKafkaEvent('ITEM_UPDATED', updated);
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/items/:id', async (req, res) => {
  const { title, description } = req.body;
  const { id } = req.params;
  try {
    const [result] = await pool.query('DELETE FROM items WHERE id = ?', [id]);
    if (result.affectedRows === 0) return res.status(404).send('Not Found');
    await sendKafkaEvent('ITEM_DELETED', { id: Number(id) });
    res.status(204).send();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 5000;
async function start() {
  await initDB();
  await producer.connect();
  app.listen(PORT, () => {
    console.log('Express server running on port ' + PORT);
  });
}

start().catch((err) => {
  console.error('Server startup failed:', err);
  process.exit(1);
});