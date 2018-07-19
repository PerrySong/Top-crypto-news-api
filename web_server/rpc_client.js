const jayson = require('jayson');

const client = jayson.client.http({
    port: 4000,
    hostname: "localhost"
})

function add(a, b, callback) {
    client.request('add', [a, b], function(err, error, response) {
        if (err) throw err;
        console.log(response)
        callback(response);
    })
}

function foobar(params , callback) {
    client.request('foobar', params, function(err, error, res) {
        if (err) throw err;
        console.log(res)
        callback(res)
    })
}

module.exports = {
    add : add,
    foobar: foobar
}