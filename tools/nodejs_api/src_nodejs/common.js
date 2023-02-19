function callbackWrapper(err, callback) {
    if (err){
        console.log("There is an error!!!" + err);
        throw err;
    } else {
        callback()
    }
}
module.exports = callbackWrapper

