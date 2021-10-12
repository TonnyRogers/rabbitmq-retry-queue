import express from "express";

import MessageQueue from './MessageKue';

const app = express();
MessageQueue.init();

app.listen(3333, () => {
    console.log('Example app listening on port 3001!');    
});