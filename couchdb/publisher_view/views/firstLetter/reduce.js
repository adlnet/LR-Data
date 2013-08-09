function(keys, values, rereduce) {
        var vals = []
        function populate(arr){
            for(var i in arr){
                var item = arr[i];
                if (typeof(item) === "string" && vals.indexOf(item) === -1){
                    vals.push(item);
                }else if (typeof(item) === "object"){
                    populate(item);
               }
            }
        }
        populate(values);
	return vals;
}
