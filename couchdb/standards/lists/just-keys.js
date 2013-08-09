function(head, req) {
    var row;
    var items = [];
    start({
        "headers": {
            "Content-Type": "application/json"
        }
    });
    while(row = getRow()){
        if(row.key){
            items.push(row.key);
        }
    }
    send(JSON.stringify(items));
}
