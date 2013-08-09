function(head, req) {
    var row = getRow();
    start({
        "headers": {
            "Content-Type": "application/json"
        }
    });
    if(row){
        send(JSON.stringify(row.value));
    }else{
        send(JSON.stringify([]));
    }
}
