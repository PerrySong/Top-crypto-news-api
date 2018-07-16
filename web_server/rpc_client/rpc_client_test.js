var client = require('./rpc_client');

client.add(1, 2, (res) => {
    console.assert(res == 3);
    console.log()
    console.log('Passed test')
})