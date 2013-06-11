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
    	var testString = ", supported by ";
    	var pos = doc.publisher.indexOf(testString);
    	if (pos !== -1 ){
    		emit(doc.publisher, null);
    	}else{
    		var newPublisher = doc.publisher.replace(testString, "||");
    		var parts = newPublisher.split("||");
    		for (var i in parts){
    			emit(parts[i].toLowerCase(), null);
    		}
    	}
    } 
}