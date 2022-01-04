const express = require('express');
const app = express();
const cors = require('cors');
const bodyParser = require('body-parser');
const dataScraper = require('./dataScraper');
const gamesRoute = require('./routes/gamesRoute');
const port = process.env.port || 5000;

async function main() {
    app.use(cors());
    await gamesRoute.Init();
    app.use('/games', gamesRoute.Router());
    app.use(bodyParser.json());
    app.listen(port, err => {
        if (err)
            console.log(err);
        console.log(`Server started.... Listening to port ${port}....`);
    });
    // await dataScraper.UpdateApplist();
    await dataScraper.UpdateAppDetails();
    // await dataScraper.UpdateAppReviews(42040);
    // await dataScraper.UpdateAppMetrics();
}
main();
