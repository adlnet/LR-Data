function(doc) {
	if(doc.url.indexOf(".gov") > 0){
		emit(doc.url, null);
	}
	
}