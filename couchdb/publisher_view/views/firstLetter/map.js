function(doc) {
  if (doc.publisher && doc.publisher.length > 0){
      var partial = "";
      for (var i in doc.publisher){
          partial = partial + doc.publisher.toLowerCase()[i];
          emit(partial, doc.publisher);
      }
  }
}
