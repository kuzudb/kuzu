const express = require('express');
const app = express();
const PORT = 3000;

// Serve static files from the 'public' folder
app.use(express.static('public'));

// Start the server
app.listen(PORT, () => {
    console.log(`Server listening on port: ${PORT}`);
});
