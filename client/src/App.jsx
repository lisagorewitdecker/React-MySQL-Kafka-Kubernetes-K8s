import React, { useEffect, useState } from 'react';

export default function App() {
    const [items, setItems] = useState([]);
    const [title, setTitle] = useState('');
    const [description, setDescription] = useState('');
    const [editingId, setEditingId] = useState(null);

    const fetchItems = async () => {
        try {
            const res = await fetch('/api/items');
            const data = await res.json();
            setItems(data);
        } catch (err) {
            console.error('Error fetching items:', err);
        }
    };

    useEffect(() => {
        fetchItems();
    }, []);

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (editingId) {
            await fetch(`/api/items/${editingId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title, description })
            });
            setEditingId(null);
        } else {
            await fetch('/api/items', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title, description })
            });
        }
        setTitle('');
        setDescription('');
        fetchItems();
    };

    const handleEdit = (item) => {
        setEditingId(item.id);
        setTitle(item.title);
        setDescription(item.description);
    };

    const handleDelete = async (id) => {
        await fetch(`/api/items/${id}`, { method: 'DELETE' });
        fetchItems();
    };

    return (
        <div style={{ maxWidth: 600, margin: '40px auto', fontFamily: 'sans-serif' }}>
            <h2>Items (MySQL + Kafka + K8s)</h2>
            <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <input
                    placeholder="Title"
                    value={title}
                    onChange={(e) => setTitle(e.target.value)}
                    required
                />
                <textarea
                    placeholder="Description"
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                    required
                />
                <button type="submit">{editingId ? 'Update Item' : 'Add Item'}</button>
            </form>

            <ul style={{ listStyleType: 'none', padding: 0, marginTop: 24 }}>
                {items.map((item) => (
                    <li key={item.id} style={{ borderBottom: '1px solid #ccc', padding: '10px 0' }}>
                        <h4>{item.title}</h4>
                        <p>{item.description}</p>
                        <button onClick={() => handleEdit(item)}>Edit</button>
                        <button onClick={() => handleDelete(item.id)} style={{ marginLeft: 8, color: 'red' }}>
                            Delete
                        </button>
                    </li>
                ))}
            </ul>
        </div>
    );
}