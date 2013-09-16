function(head, req) {
    var row;
    var items = [];
    start({
        "headers": {
            "Content-Type": "application/json"
        }
    });
    while(row = getRow()){
        if(row.value){
            items.push(row.value);
        }
    }
    send(JSON.stringify(items));
}
