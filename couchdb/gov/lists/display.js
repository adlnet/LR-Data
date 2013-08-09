function(head, req) {
    var row;
    var result = {
        count: parseInt(req.query.c)
    };
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
        }
    }
    result.data = items;
    send(JSON.stringify(result));
}
