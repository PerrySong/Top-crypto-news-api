var client = require('./rpc_client');

// invoke add
client.add(1, 2, function(res) {
    console.log(res)
    console.assert(res == 3);

});

client.foobar({foo: 'foo', 'bar': 'bar'}, (res) => {
    console.log(res)
})