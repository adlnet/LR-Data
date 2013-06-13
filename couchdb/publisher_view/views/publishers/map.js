function(doc) {
    var sanatize = function(str){
        str = str.trim()
        var loc = str.lastIndexOf(".");
        if(loc === (str.length -1 )){
            return str.substr(0, loc);
        }
        return str;
    }
    if(doc.publisher){
        emit(doc.publisher.toLowerCase(), doc.publisher);
    } 
}