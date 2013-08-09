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
            if(row.doc._attachments){
                row.doc.hasScreenshot = true;
                delete row.doc._attachments;
            }            
            items.push(row.doc);
        }else{
            items.push(row.value);
        }
    }
    send(JSON.stringify(items));
}
