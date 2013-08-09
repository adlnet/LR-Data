function(head, req) {
    var row;
    start({
        "headers": {
            "Content-Type": "application/json"
        }
    });
    var items = [];  
    while(row = getRow()){
        if(row.doc){
            items.push(row.doc);
        }
    }
    send(JSON.stringify(items));
}
