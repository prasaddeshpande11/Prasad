const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Allow large data buffers for multi-gigabyte transfers
const io = new Server(server, {
  maxHttpBufferSize: 1e9, 
  pingTimeout: 60000
});

app.use(express.static(path.join(__dirname, 'public')));

const activeVaults = new Map();

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on('setupVault', ({ pin, manifest }) => {
    activeVaults.set(pin, {
      senderSocketId: socket.id,
      manifest: manifest,
      receivers: new Set()
    });
    socket.join(pin);
    socket.emit('vault_ready', { success: true });
  });

  socket.on('join_vault', (pin) => {
    const vault = activeVaults.get(pin);
    if (!vault) {
      return socket.emit('error_message', 'Invalid PIN or vault expired.');
    }
    socket.join(pin);
    vault.receivers.add(socket.id);
    
    socket.emit('vault_manifest', vault.manifest);
    io.to(vault.senderSocketId).emit('receiver_connected', { receiverId: socket.id });
  });

  socket.on('file_chunk', (data) => {
    if (data.receiverId) {
      io.to(data.receiverId).emit('file_chunk', data);
    }
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Secure Vault server running on port ${PORT}`);
});