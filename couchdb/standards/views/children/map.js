function(doc) {
	function handleChildren(node, parents){
		var parentDescriptor = node.id;
                if (parentDescriptor){
		    for (var i in parents){
			emit(parents[i], parentDescriptor)
           	    }
                }
		if(node.children){
                        var arr = parentDescriptor ? [parentDescriptor] : [];
			for(var i in node.children){
				handleChildren(node.children[i], parents.concat([parentDescriptor]))
			}
		}
	}
 	handleChildren(doc, []);
}
