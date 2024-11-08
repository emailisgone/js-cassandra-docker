const express = require('express');
const cassandra = require('cassandra-driver');

const app = express();
const PORT = 3000;

const client = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1',
    authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra')
});

async function initDb(){
    try{
        const createKeyspaceQuery = `
            CREATE KEYSPACE IF NOT EXISTS chatsystem 
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            };
        `;
        await client.execute(createKeyspaceQuery);
        await client.execute('USE chatsystem');

        /*await client.execute('DROP TABLE IF EXISTS channels');
        await client.execute('DROP TABLE IF EXISTS messages');*/

        const createChannelTableQuery = `
            CREATE TABLE IF NOT EXISTS channels(
                id text PRIMARY KEY,
                owner text,
                topic text,
                members list<text>
            );`;
        await client.execute(createChannelTableQuery);

        const createMessagesTableQuery = `
            CREATE TABLE IF NOT EXISTS messages(
                channel_id text,
                timestamp timestamp,
                author text,
                text text,
                PRIMARY KEY(channel_id, timestamp)
            )WITH CLUSTERING ORDER BY (timestamp ASC);`;
        await client.execute(createMessagesTableQuery);
    }catch(error){
        console.error('Error initializing database:', error);
    }
}

app.use(express.json());

client.connect()
    .then(() => {
        console.log('Connected to Cassandra.');
        return initDb();
    })
    .catch((error) => {
        console.error('Error connecting to Cassandra:', error);
        process.exit(1); 
    });

app.put('/channels', async (req, res) => {
    const { id, owner, topic } = req.body;

    if(!id || !id.trim() || !owner || !owner.trim()){
        return res.status(400).json({
            message: "Invalid input, missing id or owner."
        });
    }

    try{
        const checkQuery = 'SELECT id FROM channels WHERE id = ?';
        const checkResult = await client.execute(checkQuery, [id], {prepare: true});

        if(checkResult.rowLength > 0){
            return res.status(400).json({
                message: "The channel with such id already exists."
            });
        }

        const insertQuery = 'INSERT INTO channels(id, owner, topic, members) VALUES(?, ?, ?, ?)';
        await client.execute(insertQuery, [id, owner, topic || '', [owner]], {prepare: true});

        res.status(201).json({id});
    }catch(error){
        console.error('Error processing request:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.get('/channels/:channelId', async (req, res) => {
    const {channelId} = req.params;

    try{
        const selectQuery = 'SELECT id, owner, topic FROM channels WHERE id = ?';
        const result = await client.execute(selectQuery, [channelId], {prepare: true});

        if(result.rowLength == 0){
            return res.status(404).json({
                message: "Channel not found."
            });
        }

        const channel = result.first();
        res.status(200).json({
            id: channel.id,
            owner: channel.owner,
            topic: channel.topic
        });
    }catch(error){
        console.error('Error retrieving channel:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.delete('/channels/:channelId', async (req, res) => {
    const {channelId} = req.params;

    try{
        const selectQuery = 'SELECT id FROM channels WHERE id = ?';
        const selectResult = await client.execute(selectQuery, [channelId], {prepare: true});

        if(selectResult.rowLength == 0){
            return res.status(404).json({
                message: "Channel not found."
            });
        }

        const deleteQuery = 'DELETE FROM channels WHERE id = ?';
        await client.execute(deleteQuery, [channelId], {prepare: true});

        res.status(204).send();
    }catch(error){
        console.error('Error deleting channel:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.put('/channels/:channelId/members', async (req, res) => {
    const {channelId} = req.params;
    const {member} = req.body;

    if(!member || !member.trim()){
        return res.status(400).json({
            message: "Invalid input, missing or empty member name."
        });
    }

    try{
        const selectQuery = 'SELECT members FROM channels WHERE id = ?';
        const result = await client.execute(selectQuery, [channelId], {prepare: true});

        if(result.rowLength == 0){
            return res.status(404).json({
                message: "Channel not found"
            });
        }

        const channel = result.first();
        const members = channel.members || [];

        if(members.includes(member)){
            return res.status(400).json({
                message: "Member already exists in the channel."
            });
        }

        members.push(member);

        const updateQuery = 'UPDATE channels SET members = ? WHERE id = ?';
        await client.execute(updateQuery, [members, channelId], {prepare: true});

        res.status(201).json({
            message: "Member added."
        });
    }catch(error){
        console.error('Error adding member:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.get('/channels/:channelId/members', async (req, res) => {
    const {channelId} = req.params;

    try{
        const selectQuery = 'SELECT members FROM channels WHERE id = ?';
        const result = await client.execute(selectQuery, [channelId], {prepare: true});

        if(result.rowLength == 0){
            return res.status(404).json({
                message: "Channel not found."
            });
        }

        const channel = result.first();
        const members = channel.members || [];

        res.status(200).json(members);
    }catch(error){
        console.error('Error retrieving members:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.delete('/channels/:channelId/members/:memberId', async (req, res) => {
    const {channelId, memberId} = req.params;

    try{
        const selectQuery = 'SELECT members FROM channels WHERE id = ?';
        const result = await client.execute(selectQuery, [channelId], {prepare: true});

        if(result.rowLength == 0){
            return res.status(404).json({
                message: "Channel not found."
            });
        }

        const channel = result.first();
        const members = channel.members || [];

        if(!members.includes(memberId)){
            return res.status(404).json({
                message: "Member not found."
            });
        }

        const updatedMembers = members.filter(member => member != memberId);

        const updateQuery = 'UPDATE channels SET members = ? WHERE id = ?';
        await client.execute(updateQuery, [updatedMembers, channelId], {prepare: true});

        res.status(204).json({
            message: "Member removed."
        });
    }catch(error){
        console.error('Error removing member:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.put('/channels/:channelId/messages', async (req, res) => {
    const {channelId} = req.params;
    const  {text, author} = req.body;

    if(!text || !author || !author.trim()){
        return res.status(400).json({
            message: "Invalid input, missing text or author."
        });
    }

    try{
        const timestamp = new Date();
        const insertQuery = 'INSERT INTO messages(channel_id, timestamp, author, text) VALUES(?, ?, ?, ?)';
        await client.execute(insertQuery, [channelId, timestamp, author, text], {prepare: true});

        res.status(201).json({
            message: "Message added."
        });
    }catch(error){
        console.error('Error adding message:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.get('/channels/:channelId/messages', async (req, res) => {
    const {channelId} = req.params;
    const {startAt, author} = req.query;

    try{
        const parsedStartAt = startAt ? new Date(startAt) : null;
        const query = parsedStartAt
            ? 'SELECT timestamp, author, text FROM messages WHERE channel_id = ? AND timestamp >= ?'
            : 'SELECT timestamp, author, text FROM messages WHERE channel_id = ?';
        
        const params = parsedStartAt ? [channelId, parsedStartAt] : [channelId];
        const result = await client.execute(query, params, {prepare: true});

        let messages = result.rows.map(row => ({
            text: row.text,
            author: row.author,
            timestamp: row.timestamp.toISOString()
        }));

        if(author){
            messages = messages.filter(msg => msg.author == author);
        }

        res.status(200).json(messages);
    }catch(error){
        console.error('Error retrieving messages:', error);
        res.status(500).json({
            message: 'Internal server error',
            error: error.message
        });
    }
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
